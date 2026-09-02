{#
  Decides the pivot shape -- 'scalar' | 'array' | 'nested' -- for every
  current lookup row. Mirrors python_parsing.py's two-stage decision:
  _detect_s_value_fields() (which no-m_index fields need
  ARRAY<ARRAY<STRING>>) plus normalize_arrays() (does a flat array collapse
  to a scalar). 'nested' means ARRAY(ARRAY(VARCHAR)) (Spark's Branch 2 --
  unpinned m, real s-values seen); 'array' means ARRAY(VARCHAR) (Branch 1
  with s>1, or Branch 3 with >1 m-group seen); 'scalar' means plain VARCHAR.

  If wide_relation already exists, trusts its PHYSICAL column types via
  information_schema -- because the reconcile_wide_schema run-operation is
  expected to have already committed any needed ALTER TABLE widening before
  this model runs (see macros/reconcile_wide_schema.sql). This macro
  deliberately does NOT re-derive "does this need widening" on its own for
  that case -- detection logic lives in exactly one place (the
  run-operation), so a skipped run-operation step surfaces as a real
  TYPE_MISMATCH error instead of silently mis-typing data.

  If wide_relation does not exist yet (bootstrap / --full-refresh), derives
  shapes straight from the batch, identical to reconcile_wide_schema's own
  bootstrap branch.
#}
{% macro get_column_shapes(rows, source_relation, wide_relation) %}
  {% set existing = load_relation(wide_relation) %}
  {% set shapes = {} %}

  {% if existing is not none %}
    {% set query %}
      SELECT column_name, data_type
      FROM {{ wide_relation.database }}.information_schema.columns
      WHERE table_schema = '{{ wide_relation.schema }}' AND table_name = '{{ wide_relation.identifier }}'
    {% endset %}
    {% set types = {} %}
    {% for r in run_query(query).rows %}
      {% do types.update({r['column_name']: r['data_type']}) %}
    {% endfor %}
    {% for r in rows if r.tag != 'c0' %}
      {% set dtype = types.get(r.name, '') | upper %}
      {% if dtype.startswith('ARRAY(ARRAY') %}
        {% do shapes.update({(r.tag, r.m): 'nested'}) %}
      {% elif dtype.startswith('ARRAY') %}
        {% do shapes.update({(r.tag, r.m): 'array'}) %}
      {% else %}
        {% do shapes.update({(r.tag, r.m): 'scalar'}) %}
      {% endif %}
    {% endfor %}

  {% else %}
    {#- Branch 1 (m pinned): array if max_s > 1 for that exact (tag, m) -#}
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
        FROM {{ source_relation }}
        WHERE {{ conditions | join(' OR ') }}
        GROUP BY field_index, m_index
      {% endset %}
      {% for t, m, s in run_query(query).rows %}
        {% do shapes.update({(t, m): ('array' if s and s > 1 else 'scalar')}) %}
      {% endfor %}
    {% endif %}

    {#- Branch 2/3 (m unpinned): nested if any m-group ever has s > 1;
         else array if any record ever has > 1 m-group; else scalar -#}
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
        FROM {{ source_relation }}
        WHERE field_index IN ('{{ tag_list }}')
        GROUP BY field_index
      {% endset %}
      {% for t, has_s, max_m in run_query(query).rows %}
        {% if has_s %}
          {% do shapes.update({(t, none): 'nested'}) %}
        {% elif max_m and max_m > 1 %}
          {% do shapes.update({(t, none): 'array'}) %}
        {% else %}
          {% do shapes.update({(t, none): 'scalar'}) %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endif %}

  {{ return(shapes) }}
{% endmacro %}


{#
  Ported from python_parsing.py's reconcile_iceberg_schema() Step 1:
  physical columns that exist in wide_relation but no longer have a
  matching lookup row at all (renamed/removed from lookup_metadata since
  the table was built). Excludes the fixed housekeeping columns every wide
  table always has. Returns [] if wide_relation doesn't exist yet.
#}
{% macro get_stale_columns(rows, wide_relation) %}
  {% set existing = load_relation(wide_relation) %}
  {% if existing is none %}
    {{ return([]) }}
  {% endif %}

  {% set current_names = rows | map(attribute='name') | list %}
  {% set fixed_cols = ['recid', 'xml_hash', 'ingested_at', 'source_updated_date'] %}

  {% set query %}
    SELECT column_name, data_type
    FROM {{ wide_relation.database }}.information_schema.columns
    WHERE table_schema = '{{ wide_relation.schema }}' AND table_name = '{{ wide_relation.identifier }}'
  {% endset %}

  {% set stale = [] %}
  {% for name, dtype in run_query(query).rows %}
    {% if name not in current_names and name not in fixed_cols %}
      {% do stale.append({'name': name, 'type': dtype}) %}
    {% endif %}
  {% endfor %}
  {{ return(stale) }}
{% endmacro %}
