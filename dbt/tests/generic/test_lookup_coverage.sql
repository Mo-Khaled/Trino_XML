{#
  Replaces gen_sql.py's --mode reconcile: with EAV output there's no schema
  to ALTER (see gen_sql.py's mode_reconcile docstring) -- what can still
  drift is coverage, a tag present in real data with no lookup row naming
  it. A failing row here means dbt test surfaces that gap in CI instead of
  someone having to remember to run a manual report.
#}
{% test lookup_coverage(model, table_name) %}

SELECT DISTINCT a.field_index
FROM {{ model }} AS a
LEFT JOIN {{ source('bronze', 'lookup_metadata') }} AS l
  ON l.table_name = '{{ table_name }}' AND l.field_index = a.field_index
WHERE l.field_index IS NULL

{% endtest %}
