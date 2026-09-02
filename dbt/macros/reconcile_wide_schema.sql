{#
  dbt run-operation port of python_parsing.py's reconcile_iceberg_schema() /
  _migrate_table_column() Step 3 (type-mismatch resolution) -- the only step
  that needs real DDL. Deliberately NOT a model or a pre_hook -- see
  dbt/README.md for why. Invoke with:

    dbt run-operation reconcile_wide_schema --args '{table_name: account}'

  immediately before `dbt run --select account_wide`. account_wide's own
  get_column_shapes() macro trusts the PHYSICAL column types this leaves
  behind -- it does not re-derive the widening decision itself.

  Shapes only ever widen here (scalar -> array -> nested), mirroring
  Spark's actual behavior: reconcile_iceberg_schema() only ever migrates
  the TABLE when the batch is wider than it; the reverse ("table already
  wider than what this batch needs") is handled by building the wider
  expression anyway in get_wide_select.sql, the same way Spark's Cases D/E/F
  cast the DataFrame up to match the table rather than narrowing the table.
  Step 1 (columns in the table but missing from the batch/lookup) and Step 2
  (new columns) aren't handled here -- Step 2 is dbt's own
  on_schema_change='append_new_columns', and Step 1 is get_wide_select's
  stale-column NULL-fill (get_stale_columns) -- neither needs DDL up front.
#}

{% macro reconcile_wide_schema(table_name) %}
  {% if not execute %}
    {{ return(none) }}
  {% endif %}

  {% set shape_rank = {'scalar': 0, 'array': 1, 'nested': 2} %}
  {% set wide_relation = ref(table_name ~ '_wide') %}
  {% set attrs_relation = ref(table_name ~ '_attributes') %}
  {% set existing = adapter.get_relation(wide_relation.database, wide_relation.schema, wide_relation.identifier) %}

  {% if existing is none %}
    {{ log('reconcile_wide_schema: ' ~ wide_relation ~ ' does not exist yet -- nothing to migrate', info=True) }}
    {{ return(none) }}
  {% endif %}

  {% set rows = get_lookup_rows(table_name) %}
  {% if rows | length == 0 %}
    {{ return(none) }}
  {% endif %}

  {#-- batch_shapes: what shape does TODAY's batch need, per lookup row?
       Same detection get_column_shapes.sql's bootstrap branch uses. #}
  {% set batch_shapes = {} %}

  {% set pinned_pairs = [] %}
  {% for r in rows if r.tag != 'c0' and r.m is not none %}
    {% if (r.tag, r.m) not in pinned_pairs %}
      {% do pinned_pairs.append((r.tag, r.m)) %}
    {% endif %}
  {% endfor %}
  {% if pinned_pairs | length > 0 %}
    {% set conditions = [] %}
    {% for t, m in pinned_pairs %}
      {% do conditions.append("(field_index = '" ~ t ~ "' AND m_index = " ~ m ~ ")") %}
    {% endfor %}
    {% set query %}
      SELECT field_index, m_index, max(s_index) AS max_s
      FROM {{ attrs_relation }}
      WHERE {{ conditions | join(' OR ') }}
      GROUP BY field_index, m_index
    {% endset %}
    {% for t, m, s in run_query(query).rows %}
      {% do batch_shapes.update({(t, m): ('array' if s and s > 1 else 'scalar')}) %}
    {% endfor %}
  {% endif %}

  {% set unpinned_tags = [] %}
  {% for r in rows if r.tag != 'c0' and r.m is none %}
    {% if r.tag not in unpinned_tags %}
      {% do unpinned_tags.append(r.tag) %}
    {% endif %}
  {% endfor %}
  {% if unpinned_tags | length > 0 %}
    {% set tag_list = unpinned_tags | map('string') | map('replace', "'", "''") | join("', '") %}
    {% set query %}
      SELECT field_index, bool_or(s_index > 1) AS has_s, max(m_index) AS max_m
      FROM {{ attrs_relation }}
      WHERE field_index IN ('{{ tag_list }}')
      GROUP BY field_index
    {% endset %}
    {% for t, has_s, max_m in run_query(query).rows %}
      {% if has_s %}
        {% do batch_shapes.update({(t, none): 'nested'}) %}
      {% elif max_m and max_m > 1 %}
        {% do batch_shapes.update({(t, none): 'array'}) %}
      {% else %}
        {% do batch_shapes.update({(t, none): 'scalar'}) %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {#-- current physical shape per lookup row #}
  {% set type_query %}
    SELECT column_name, data_type
    FROM {{ wide_relation.database }}.information_schema.columns
    WHERE table_schema = '{{ wide_relation.schema }}' AND table_name = '{{ wide_relation.identifier }}'
  {% endset %}
  {% set table_types = {} %}
  {% for name, dtype in run_query(type_query).rows %}
    {% do table_types.update({name: dtype}) %}
  {% endfor %}

  {% set name_by_key = {} %}
  {% for r in rows %}
    {% do name_by_key.update({(r.tag, r.m): r.name}) %}
  {% endfor %}

  {#-- migrate any lookup row whose batch shape is wider than its current
       physical shape -- mirrors reconcile_iceberg_schema()'s Cases A/B/C #}
  {% set migrated = [] %}
  {% for key, target_shape in batch_shapes.items() %}
    {% set name = name_by_key.get(key) %}
    {% if name is not none %}
      {% set dtype = table_types.get(name, '') | upper %}
      {% set current_shape = 'nested' if dtype.startswith('ARRAY(ARRAY') else ('array' if dtype.startswith('ARRAY') else 'scalar') %}
      {% if name in table_types and shape_rank[target_shape] > shape_rank[current_shape] %}
        {% do migrate_wide_column(wide_relation, name, current_shape, target_shape) %}
        {% do migrated.append(name ~ ' (' ~ current_shape ~ ' -> ' ~ target_shape ~ ')') %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% if migrated | length > 0 %}
    {{ log('reconcile_wide_schema: migrated ' ~ migrated, info=True) }}
  {% else %}
    {{ log('reconcile_wide_schema: no columns needed widening', info=True) }}
  {% endif %}
{% endmacro %}


{#
  add -> copy -> drop -> rename, mirroring python_parsing.py's
  _migrate_table_column(). Iceberg supports all four natively. The
  update_expr (how to backfill the new column from the old one) depends on
  which of the three widening transitions this is -- mirrors
  reconcile_iceberg_schema()'s Cases A (scalar->nested), B (array->nested),
  C (scalar->array).
#}
{% macro migrate_wide_column(wide_relation, col_name, from_shape, to_shape) %}
  {% set tmp = col_name ~ '_v2' %}

  {% if to_shape == 'array' %}
    {% set to_type_sql = 'ARRAY(VARCHAR)' %}
    {% set update_expr = 'CASE WHEN "' ~ col_name ~ '" IS NULL THEN NULL ELSE ARRAY["' ~ col_name ~ '"] END' %}
  {% elif from_shape == 'scalar' %}
    {#- scalar -> nested: Case A, two levels of wrapping -#}
    {% set to_type_sql = 'ARRAY(ARRAY(VARCHAR))' %}
    {% set update_expr = 'CASE WHEN "' ~ col_name ~ '" IS NULL THEN NULL ELSE ARRAY[ARRAY["' ~ col_name ~ '"]] END' %}
  {% else %}
    {#- array -> nested: Case B, wrap each existing element -#}
    {% set to_type_sql = 'ARRAY(ARRAY(VARCHAR))' %}
    {% set update_expr = 'CASE WHEN "' ~ col_name ~ '" IS NULL THEN NULL ELSE transform("' ~ col_name ~ '", x -> ARRAY[x]) END' %}
  {% endif %}

  {{ log('  migrating "' ~ col_name ~ '": ' ~ from_shape ~ ' -> ' ~ to_shape, info=True) }}
  {% do run_query('ALTER TABLE ' ~ wide_relation ~ ' ADD COLUMN "' ~ tmp ~ '" ' ~ to_type_sql) %}
  {% do run_query('UPDATE ' ~ wide_relation ~ ' SET "' ~ tmp ~ '" = ' ~ update_expr) %}
  {% do run_query('ALTER TABLE ' ~ wide_relation ~ ' DROP COLUMN "' ~ col_name ~ '"') %}
  {% do run_query('ALTER TABLE ' ~ wide_relation ~ ' RENAME COLUMN "' ~ tmp ~ '" TO "' ~ col_name ~ '"') %}
{% endmacro %}
