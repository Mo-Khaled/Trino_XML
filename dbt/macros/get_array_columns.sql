{#
  Decides which (tag, m) pairs need the array-valued pivot expression.

  If wide_relation already exists, trusts its PHYSICAL column types via
  information_schema -- because the reconcile_wide_schema run-operation is
  expected to have already committed any needed ALTER TABLE widening before
  this model runs (see macros/reconcile_wide_schema.sql). This macro
  deliberately does NOT re-derive "does this need widening" on its own for
  that case -- detection logic lives in exactly one place (the
  run-operation), so a skipped run-operation step surfaces as a real
  TYPE_MISMATCH error instead of silently mis-typing data.

  If wide_relation does not exist yet (bootstrap / --full-refresh), derives
  array-vs-scalar straight from the batch, identical to
  reconcile_wide_schema.py's bootstrap branch (detect_batch_shapes() +
  "needs_array = max_s > 1").
#}
{% macro get_array_columns(rows, source_relation, wide_relation) %}
  {% set existing = load_relation(wide_relation) %}
  {% set array_cols = [] %}

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
    {% for r in rows %}
      {% if types.get(r.name, '').upper().startswith('ARRAY') %}
        {% do array_cols.append((r.tag, r.m)) %}
      {% endif %}
    {% endfor %}

  {% else %}
    {% set pairs = [] %}
    {% for r in rows if r.tag != 'c0' %}
      {% if (r.tag, r.m) not in pairs %}
        {% do pairs.append((r.tag, r.m)) %}
      {% endif %}
    {% endfor %}
    {% if pairs | length > 0 %}
      {% set conditions = [] %}
      {% for t, m in pairs %}
        {% do conditions.append("(field_index = '" ~ t ~ "' AND m_index = " ~ m ~ ")") %}
      {% endfor %}
      {% set query %}
        SELECT field_index, m_index, max(s_index) AS max_s
        FROM {{ source_relation }}
        WHERE {{ conditions | join(' OR ') }}
        GROUP BY field_index, m_index
      {% endset %}
      {% for t, m, s in run_query(query).rows %}
        {% if s and s > 1 %}
          {% do array_cols.append((t, m)) %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endif %}

  {{ return(array_cols) }}
{% endmacro %}
