{#
  dbt run-operation port of reconcile_wide_schema.py's detect_batch_shapes() /
  read_table_types() / reconcile() / migrate_to_array(). Deliberately NOT a
  model or a pre_hook -- see dbt/README.md for why. Invoke with:

    dbt run-operation reconcile_wide_schema --args '{table_name: account}'

  immediately before `dbt run --select account_wide`. account_wide's own
  get_array_columns() macro trusts the PHYSICAL column types this leaves
  behind -- it does not re-derive the widening decision itself.
#}
{% macro reconcile_wide_schema(table_name) %}
  {% if not execute %}
    {{ return(none) }}
  {% endif %}

  {% set wide_relation = ref(table_name ~ '_wide') %}
  {% set attrs_relation = ref(table_name ~ '_attributes') %}
  {% set existing = adapter.get_relation(wide_relation.database, wide_relation.schema, wide_relation.identifier) %}

  {% if existing is none %}
    {{ log('reconcile_wide_schema: ' ~ wide_relation ~ ' does not exist yet -- nothing to migrate', info=True) }}
    {{ return(none) }}
  {% endif %}

  {% set rows = get_lookup_rows(table_name) %}
  {% set pairs = [] %}
  {% for r in rows if r.tag != 'c0' %}
    {% if (r.tag, r.m) not in pairs %}
      {% do pairs.append((r.tag, r.m)) %}
    {% endif %}
  {% endfor %}
  {% if pairs | length == 0 %}
    {{ return(none) }}
  {% endif %}

  {#-- detect_batch_shapes(): does account_attributes currently have s_index > 1
       for a given lookup row's (field_index, m_index)? #}
  {% set conditions = [] %}
  {% for t, m in pairs %}
    {% do conditions.append("(field_index = '" ~ t ~ "' AND m_index = " ~ m ~ ")") %}
  {% endfor %}
  {% set shape_query %}
    SELECT field_index, m_index, max(s_index) AS max_s
    FROM {{ attrs_relation }}
    WHERE {{ conditions | join(' OR ') }}
    GROUP BY field_index, m_index
  {% endset %}
  {% set batch_shapes = {} %}
  {% for t, m, s in run_query(shape_query).rows %}
    {% do batch_shapes.update({(t, m): s}) %}
  {% endfor %}

  {#-- read_table_types(): account_wide's current column types #}
  {% set type_query %}
    SELECT column_name, data_type
    FROM {{ wide_relation.database }}.information_schema.columns
    WHERE table_schema = '{{ wide_relation.schema }}' AND table_name = '{{ wide_relation.identifier }}'
  {% endset %}
  {% set table_types = {} %}
  {% for name, dtype in run_query(type_query).rows %}
    {% do table_types.update({name: dtype}) %}
  {% endfor %}

  {% set name_by_tag_m = {} %}
  {% for r in rows %}
    {% do name_by_tag_m.update({(r.tag, r.m): r.name}) %}
  {% endfor %}

  {#-- reconcile(): for any lookup row whose batch shape is wider than the
       table's current column, migrate -- mirrors _migrate_table_column() #}
  {% set migrated = [] %}
  {% for key, max_s in batch_shapes.items() %}
    {% set name = name_by_tag_m.get(key) %}
    {% if name is not none %}
      {% set current_type = table_types.get(name) %}
      {% set is_array_now = current_type is not none and current_type.upper().startswith('ARRAY') %}
      {% set needs_array = max_s is not none and max_s > 1 %}
      {% if needs_array and current_type is not none and not is_array_now %}
        {% do migrate_wide_column_to_array(wide_relation, name) %}
        {% do migrated.append(name) %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {% if migrated | length > 0 %}
    {{ log('reconcile_wide_schema: migrated to ARRAY(VARCHAR): ' ~ migrated, info=True) }}
  {% else %}
    {{ log('reconcile_wide_schema: no columns needed widening', info=True) }}
  {% endif %}
{% endmacro %}


{#
  add -> copy -> drop -> rename, mirroring reconcile_wide_schema.py's
  migrate_to_array(). Iceberg supports all four natively.
#}
{% macro migrate_wide_column_to_array(wide_relation, col_name) %}
  {% set tmp = col_name ~ '_v2' %}
  {{ log('  migrating "' ~ col_name ~ '": VARCHAR -> ARRAY(VARCHAR)', info=True) }}
  {% do run_query('ALTER TABLE ' ~ wide_relation ~ ' ADD COLUMN "' ~ tmp ~ '" ARRAY(VARCHAR)') %}
  {% do run_query('UPDATE ' ~ wide_relation ~ ' SET "' ~ tmp ~ '" = CASE WHEN "' ~ col_name ~ '" IS NULL THEN NULL ELSE ARRAY["' ~ col_name ~ '"] END') %}
  {% do run_query('ALTER TABLE ' ~ wide_relation ~ ' DROP COLUMN "' ~ col_name ~ '"') %}
  {% do run_query('ALTER TABLE ' ~ wide_relation ~ ' RENAME COLUMN "' ~ tmp ~ '" TO "' ~ col_name ~ '"') %}
{% endmacro %}
