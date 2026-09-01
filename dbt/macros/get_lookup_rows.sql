{#
  Live counterpart to gen_sql.py's load_lookup_csv() -- reads the same
  (field_index, m_index, resolved_name_en) shape, but from the live
  source table instead of a CSV export, via run_query() at compile time.
  Shared by account_wide.sql and the reconcile_wide_schema run-operation --
  single source of truth, unlike the two Python scripts each having their
  own copy of this logic.
#}
{% macro get_lookup_rows(table_name) %}
  {% if not execute %}
    {{ return([]) }}
  {% endif %}
  {% set query %}
    SELECT field_index, coalesce(m_index, 1) AS m_index, resolved_name_en
    FROM {{ source('bronze', 'lookup_metadata') }}
    WHERE table_name = '{{ table_name }}'
    ORDER BY field_index, m_index
  {% endset %}
  {% set results = run_query(query) %}
  {% set rows = [] %}
  {% for row in results.rows %}
    {% do rows.append({'tag': row['field_index'], 'm': row['m_index'] | int, 'name': row['resolved_name_en']}) %}
  {% endfor %}
  {{ return(rows) }}
{% endmacro %}
