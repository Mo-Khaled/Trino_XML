{#
  Ported from gen_sql.py's mode_ingest token-completeness check: matched_tags
  (after self-closing-tag normalization) must equal raw_tag_opens. A mismatch
  means TAG_RE is silently missing an element shape.
#}
SELECT
  count(*) AS records,
  sum(cardinality(regexp_extract_all(
    regexp_replace(xmlrecord, '{{ self_close_re() }}', '{{ self_close_replacement() }}'),
    '{{ tag_re() }}', 1
  ))) AS matched_tags,
  sum(cardinality(regexp_extract_all(xmlrecord, '<c\d+[ />]'))) AS raw_tag_opens
FROM {{ ref('customer_raw') }}
HAVING sum(cardinality(regexp_extract_all(
    regexp_replace(xmlrecord, '{{ self_close_re() }}', '{{ self_close_replacement() }}'),
    '{{ tag_re() }}', 1
  ))) != sum(cardinality(regexp_extract_all(xmlrecord, '<c\d+[ />]')))
