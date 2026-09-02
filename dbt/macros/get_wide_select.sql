{#
  Per-column pivot expressions -- 1:1 port of python_parsing.py's
  _build_select_expressions()'s three branches, plus normalize_arrays()'s
  scalar collapse. Which branch a lookup row uses is decided by whether its
  m_index is pinned (Branch 1) or not (Branch 2/3); which of Branch 2 vs 3
  applies, and whether a column collapses to scalar, is decided by
  get_column_shapes.sql -- these macros only render the expression for a
  shape that's already been decided.

  Branch 1 (m pinned)     -- element_at(f, 'tag_m') scalar, or the s-indexed
                              array built from g, exactly as before.
  Branch 2 (m unpinned,    -- ARRAY(ARRAY(VARCHAR)): outer index = m-group,
    has real s-values)        inner index = s-slot within that m-group.
                               Mirrors _build_select_expressions()'s Branch 2
                               exactly, including the sparse-m-group guard
                               (T24 can skip an m-group entirely; sequence(1,0)
                               throws in Trino, hence the cardinality check).
  Branch 3 (m unpinned,    -- ARRAY(VARCHAR): one value per m-group. Can
    no real s-values)         still collapse to scalar if every record's
                               batch never has more than one m-group for
                               that field (mirrors normalize_arrays()'s
                               single-element-array flatten) -- the scalar
                               form is literally element_at() of the array
                               form, same two-pass shape Spark uses
                               (_build_select_expressions builds the array,
                               normalize_arrays may then collapse it),
                               including its edge cases.
#}

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
