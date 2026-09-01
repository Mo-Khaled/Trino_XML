
{% macro unescape_expr(inner) %}
{%- set entities = [
    ("&lt;", "<"), ("&gt;", ">"), ("&quot;", '"'),
    ("&apos;", "'"), ("&#39;", "'"), ("&amp;", "&")
] -%}
{%- set ns = namespace(e=inner) -%}
{%- for ent, ch in entities -%}
  {%- set lit = ch.replace("'", "''") -%}
  {%- set ns.e = "replace(" ~ ns.e ~ ", '" ~ ent ~ "', '" ~ lit ~ "')" -%}
{%- endfor -%}
{{- ns.e -}}
{%- endmacro %}


{% macro blank_to_null(check_expr, value_expr=none) %}
{%- if value_expr is none -%}
  {%- set value_expr = check_expr -%}
{%- endif -%}
CASE WHEN {{ check_expr }} = '' THEN NULL ELSE {{ value_expr }} END
{%- endmacro %}


{% macro token_ctes(source) %}
{%- set tag_re = '<(c\\d+)([^>]*)>([^<]*)</c\\d+>' -%}
{%- set m_re = 'm="(\\d+)"' -%}
{%- set s_re = 's="(\\d+)"' -%}
{%- set self_close_re = '<(c\\d+)([^>]*)/>' -%}
{%- set self_close_replacement = '<$1$2></$1>' -%}
{%- set token_row = 'ROW(tag VARCHAR, m INTEGER, s INTEGER, val VARCHAR)' -%}
{%- set val = unescape_expr("e[3]") -%}
-- Part 1/3 -- read XML, hash it, normalize self-closing empty tags.
WITH raw AS (
  SELECT
    recid,
    xmlrecord,
    to_hex(md5(to_utf8(xmlrecord))) AS xml_hash,
    -- <c100/> (Oracle's serialized form of an empty field) -> <c100></c100>.
    regexp_replace(xmlrecord, '{{ self_close_re }}', '{{ self_close_replacement }}') AS xmlrecord_norm
  FROM {{ source }}
),

-- Part 2/3 -- extract every tag into one (tag, m, s, value) struct per
-- occurrence, one array per record. 3 scans total, not one per field.
tokens AS (
  SELECT
    recid,
    xml_hash,
    transform(
      zip(
        regexp_extract_all(xmlrecord_norm, '{{ tag_re }}', 1),
        regexp_extract_all(xmlrecord_norm, '{{ tag_re }}', 2),
        regexp_extract_all(xmlrecord_norm, '{{ tag_re }}', 3)
      ),
      e -> CAST(
        ROW(
          e[1],
          CAST(COALESCE(regexp_extract(e[2], '{{ m_re }}', 1), '1') AS INTEGER),
          CAST(COALESCE(regexp_extract(e[2], '{{ s_re }}', 1), '1') AS INTEGER),
          {{ blank_to_null("e[3]", val) }}
        ) AS {{ token_row }}
      )
    ) AS entries
  FROM raw
),

-- Part 3/3 -- one row per tag occurrence. Sentinel keeps a no-tag record's
-- recid alive (UNNEST on an empty array drops the row); filtered out by callers.
exploded AS (
  SELECT t.recid, t.xml_hash, u.tag, u.m, u.s, u.val
  FROM tokens t
  CROSS JOIN UNNEST(
    IF(cardinality(t.entries) = 0,
       ARRAY[CAST(ROW('__empty__', 1, 1, NULL) AS {{ token_row }})],
       t.entries)
  ) AS u(tag, m, s, val)
)
{%- endmacro %}


{#- These are plain template text, not Jinja string literals -- a backslash
    here is NOT escape-processed the way it is inside {% set x = '...' %},
    so it must be a single literal backslash, not \\. -#}
{%- macro self_close_re() -%}<(c\d+)([^>]*)/>{%- endmacro -%}
{%- macro self_close_replacement() -%}<$1$2></$1>{%- endmacro -%}
{%- macro tag_re() -%}<(c\d+)([^>]*)>([^<]*)</c\d+>{%- endmacro -%}
