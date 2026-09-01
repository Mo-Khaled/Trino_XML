{#
  1:1 ports of reconcile_wide_schema.py's build_columns() helpers.
#}

{% macro wide_scalar_expr(tag, m) -%}
element_at(f, '{{ tag }}_{{ m }}')
{%- endmacro %}

{% macro wide_array_expr(tag, m) -%}
{%- set key = tag ~ '_' ~ m -%}
{%- set g = "element_at(g, '" ~ key ~ "')" -%}
CASE WHEN {{ g }} IS NULL THEN NULL ELSE transform(sequence(1, array_max(transform({{ g }}, x -> x.s))), i -> element_at(transform(filter({{ g }}, x -> x.s = i), x -> x.val), 1)) END
{%- endmacro %}

{#
  Renders the dynamic per-lookup-row column list -- the mechanism that
  replaces gen_sql.py's Python-side codegen with a compile-time run_query()
  against the live lookup table. c0 is special-cased to recid (it's the
  <row id=...> XML attribute, not a <c0> element). Returns
  (columns_sql, ordered_column_names) -- names are needed by callers that
  must build an explicit INSERT column list (see account_wide.sql's comment
  on why SELECT * is unsafe after a schema-widening migration).
#}
{% macro get_wide_select(rows, array_cols, watermark_field) %}
  {%- set columns = ['  recid'] -%}
  {%- set names = ['recid'] -%}
  {%- for r in rows -%}
    {%- set col = '"' ~ r.name.replace('"', '""') ~ '"' -%}
    {%- do names.append(r.name) -%}
    {%- if r.tag == 'c0' -%}
      {%- do columns.append('  recid AS ' ~ col) -%}
    {%- elif (r.tag, r.m) in array_cols -%}
      {%- do columns.append('  ' ~ wide_array_expr(r.tag, r.m) ~ ' AS ' ~ col) -%}
    {%- else -%}
      {%- do columns.append('  ' ~ wide_scalar_expr(r.tag, r.m) ~ ' AS ' ~ col) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- do columns.append("  TRY(CAST(date_parse(element_at(f, '" ~ watermark_field ~ "_1'), '%Y%m%d') AS DATE)) AS source_updated_date") -%}
  {%- do names.append('source_updated_date') -%}
  {{ return((columns | join(',\n'), names)) }}
{% endmacro %}


{#
  Same map-once pattern as gen_sql.py's wide_pivot_cte, plus a second,
  multimap_agg-grouped structure (g) alongside the scalar one (f) -- g
  keeps every (s, value) pair per key instead of collapsing to one, which
  scalar columns don't need but array columns do.
#}
{% macro wide_pivot_cte(source) %}
WITH grouped AS (
  SELECT
    recid,
    map_agg(field_index || '_' || CAST(m_index AS VARCHAR), field_value) AS f,
    multimap_agg(
      field_index || '_' || CAST(m_index AS VARCHAR),
      CAST(ROW(s_index, field_value) AS ROW(s INTEGER, val VARCHAR))
    ) AS g,
    max(xml_hash) AS xml_hash
  FROM {{ source }}
  GROUP BY recid
)
{%- endmacro %}
