{#
  Live counterpart to gen_sql.py's load_lookup_csv() -- reads the same
  (field_index, m_index, resolved_name_en) shape, but from the live
  source table instead of a CSV export, via run_query() at compile time.
  Shared by account_wide.sql and the reconcile_wide_schema run-operation --
  single source of truth, unlike the two Python scripts each having their
  own copy of this logic.

  m_index is preserved as-is, INCLUDING none for a blank lookup row --
  unlike earlier versions of this macro, a blank m_index is no longer
  coalesced to 1. That coalesce meant "give me only the first m-group
  occurrence," which silently diverged from python_parsing.py's actual
  semantics: a lookup row with no m_index means "give me every m-group
  occurrence for this field" (Branch 2/3 in get_wide_select.sql), not
  "assume m=1." r.m is None for exactly those rows now.
#}
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
