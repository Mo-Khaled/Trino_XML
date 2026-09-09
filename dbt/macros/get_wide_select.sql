
{% macro wide_scalar_expr(tag, m) -%}
element_at(f, '{{ tag }}_{{ m }}')
{%- endmacro %}

{% macro wide_array_expr(tag, m) -%}
{%- set key = tag ~ '_' ~ m -%}
{%- set g = "element_at(g, '" ~ key ~ "')" -%}
CASE WHEN {{ g }} IS NULL THEN NULL ELSE transform(sequence(1, array_max(transform({{ g }}, x -> x.s))), i -> element_at(transform(filter({{ g }}, x -> x.s = i), x -> x.val), 1)) END
{%- endmacro %}

{% macro wide_branch3_array_expr(tag) -%}
{%- set h = "element_at(h, '" ~ tag ~ "')" -%}
CASE WHEN {{ h }} IS NULL THEN NULL ELSE transform(sequence(1, array_max(transform({{ h }}, x -> x.m))), i -> element_at(transform(filter({{ h }}, x -> x.m = i), x -> x.val), 1)) END
{%- endmacro %}

{% macro wide_branch3_scalar_expr(tag) -%}
element_at({{ wide_branch3_array_expr(tag) }}, 1)
{%- endmacro %}

{% macro wide_branch2_expr(tag) -%}
{%- set h = "element_at(h, '" ~ tag ~ "')" -%}
CASE WHEN {{ h }} IS NULL THEN NULL ELSE transform(
  sequence(1, array_max(transform({{ h }}, x -> x.m))),
  i -> IF(cardinality(filter({{ h }}, x -> x.m = i)) = 0,
       CAST(ARRAY[] AS ARRAY(VARCHAR)),
       transform(
         sequence(1, array_max(transform(filter({{ h }}, x -> x.m = i), x -> x.s))),
         j -> element_at(transform(filter({{ h }}, x -> x.m = i AND x.s = j), x -> x.val), 1)
       ))
) END
{%- endmacro %}

{% macro stale_column_fill_expr(col_type) -%}
{%- if col_type.upper().startswith('ARRAY') -%}
CAST(ARRAY[] AS {{ col_type }})
{%- else -%}
CAST(NULL AS {{ col_type }})
{%- endif -%}
{%- endmacro %}


{#
  Renders the dynamic per-lookup-row column list -- the mechanism that
  replaces gen_sql.py's Python-side codegen with a compile-time run_query()
  against the live lookup table. c0 is special-cased to recid (it's the
  <row id=...> XML attribute, not a <c0> element).

  shapes: dict {(tag, m_or_none): 'scalar' | 'array' | 'nested'} from
  get_column_shapes.sql, one entry per current lookup row.

  stale_columns: [{'name':.., 'type':..}] -- physical columns in the wide
  table that no longer have a lookup row at all (renamed/removed from
  lookup_metadata since the table was built). Ported from
  python_parsing.py's reconcile_iceberg_schema() Step 1: rather than
  silently omitting these from the SELECT (which would still work --
  Trino's INSERT INTO target (subset of cols) leaves the rest NULL on
  new/changed rows -- but isn't what Spark's code actually does), each one
  gets an explicit fill: an empty array for array-typed stale columns
  (F.array().cast(table_type)), NULL for scalar ones (F.lit(None).cast(...)).

  Returns (columns_sql, ordered_column_names) -- names are needed by callers
  that must build an explicit INSERT column list.
#}
{% macro get_wide_select(rows, shapes, stale_columns, watermark_field) %}
  {%- set columns = ['  recid'] -%}
  {%- set names = ['recid'] -%}
  {%- for r in rows -%}
    {%- set col = '"' ~ r.name.replace('"', '""') ~ '"' -%}
    {%- do names.append(r.name) -%}
    {%- if r.tag == 'c0' -%}
      {%- do columns.append('  recid AS ' ~ col) -%}
    {%- elif r.m is not none -%}
      {#- Branch 1: m pinned -#}
      {%- if shapes.get((r.tag, r.m)) == 'array' -%}
        {%- do columns.append('  ' ~ wide_array_expr(r.tag, r.m) ~ ' AS ' ~ col) -%}
      {%- else -%}
        {%- do columns.append('  ' ~ wide_scalar_expr(r.tag, r.m) ~ ' AS ' ~ col) -%}
      {%- endif -%}
    {%- else -%}
      {#- Branch 2/3: m unpinned -#}
      {%- set shape = shapes.get((r.tag, r.m)) -%}
      {%- if shape == 'nested' -%}
        {%- do columns.append('  ' ~ wide_branch2_expr(r.tag) ~ ' AS ' ~ col) -%}
      {%- elif shape == 'array' -%}
        {%- do columns.append('  ' ~ wide_branch3_array_expr(r.tag) ~ ' AS ' ~ col) -%}
      {%- else -%}
        {%- do columns.append('  ' ~ wide_branch3_scalar_expr(r.tag) ~ ' AS ' ~ col) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for sc in stale_columns -%}
    {%- set col = '"' ~ sc.name.replace('"', '""') ~ '"' -%}
    {%- do names.append(sc.name) -%}
    {%- do columns.append('  ' ~ stale_column_fill_expr(sc.type) ~ ' AS ' ~ col) -%}
  {%- endfor -%}
  {%- do columns.append("  TRY(CAST(date_parse(element_at(f, '" ~ watermark_field ~ "_1'), '%Y%m%d') AS DATE)) AS source_updated_date") -%}
  {%- do names.append('source_updated_date') -%}
  {{ return((columns | join(',\n'), names)) }}
{% endmacro %}


{#
  Map-once pattern, three maps per recid instead of one:
    f -- scalar lookup, keyed by "tag_m" (Branch 1 scalar columns)
    g -- every (s, value) pair, keyed by "tag_m" (Branch 1 array columns)
    h -- every (m, s, value) triple, keyed by "tag" alone (Branch 2/3 --
         unpinned-m columns need every m-group, not just one)
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
    multimap_agg(
      field_index,
      CAST(ROW(m_index, s_index, field_value) AS ROW(m INTEGER, s INTEGER, val VARCHAR))
    ) AS h,
    max(xml_hash) AS xml_hash
  FROM {{ source }}
  GROUP BY recid
)
{%- endmacro %}
