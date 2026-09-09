
{% macro get_lookup_rows(table_name) %}
  {% if not execute %}
    {{ return([]) }}
  {% endif %}
  {% set query %}
    SELECT field_index, m_index, resolved_name_en
    FROM {{ source('bronze', 'lookup_metadata') }}
    WHERE table_name = '{{ table_name }}'
    ORDER BY field_index, m_index
  {% endset %}
  {% set results = run_query(query) %}
  {% set rows = [] %}
  {% for row in results.rows %}
    {% set m = row['m_index'] | int if row['m_index'] is not none else none %}
    {% do rows.append({'tag': row['field_index'], 'm': m, 'name': row['resolved_name_en']}) %}
  {% endfor %}
  {{ return(rows) }}
{% endmacro %}
