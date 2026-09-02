

-- Part 1/3 -- read XML, hash it, normalize self-closing empty tags.
WITH raw AS (
  SELECT
    recid,
    xmlrecord,
    to_hex(md5(to_utf8(xmlrecord))) AS xml_hash,
    -- <c100/> (Oracle's serialized form of an empty field) -> <c100></c100>.
    regexp_replace(xmlrecord, '<(c\d+)([^>]*)/>', '<$1$2></$1>') AS xmlrecord_norm
  FROM "iceberg"."staging"."account_raw"
),

-- Part 2/3 -- extract every tag into one (tag, m, s, value) struct per
-- occurrence, one array per record. 3 scans total, not one per field.
tokens AS (
  SELECT
    recid,
    xml_hash,
    transform(
      zip(
        regexp_extract_all(xmlrecord_norm, '<(c\d+)([^>]*)>([^<]*)</c\d+>', 1),
        regexp_extract_all(xmlrecord_norm, '<(c\d+)([^>]*)>([^<]*)</c\d+>', 2),
        regexp_extract_all(xmlrecord_norm, '<(c\d+)([^>]*)>([^<]*)</c\d+>', 3)
      ),
      e -> CAST(
        ROW(
          e[1],
          CAST(COALESCE(regexp_extract(e[2], 'm="(\d+)"', 1), '1') AS INTEGER),
          CAST(COALESCE(regexp_extract(e[2], 's="(\d+)"', 1), '1') AS INTEGER),
          CASE WHEN e[3] = '' THEN NULL ELSE replace(replace(replace(replace(replace(replace(e[3], '&lt;', '<'), '&gt;', '>'), '&quot;', '"'), '&apos;', ''''), '&#39;', ''''), '&amp;', '&') END
        ) AS ROW(tag VARCHAR, m INTEGER, s INTEGER, val VARCHAR)
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
       ARRAY[CAST(ROW('__empty__', 1, 1, NULL) AS ROW(tag VARCHAR, m INTEGER, s INTEGER, val VARCHAR))],
       t.entries)
  ) AS u(tag, m, s, val)
)
,
changed AS (
  SELECT DISTINCT e.recid
  FROM exploded e
  LEFT JOIN (SELECT DISTINCT recid, xml_hash FROM "iceberg"."staging"."account_attributes") t
    ON t.recid = e.recid
  WHERE t.recid IS NULL OR t.xml_hash IS DISTINCT FROM e.xml_hash
)
SELECT
  recid,
  tag AS field_index,
  m AS m_index,
  s AS s_index,
  val AS field_value,
  xml_hash,
  current_timestamp AS ingested_at
FROM exploded
WHERE tag != '__empty__'
  AND recid IN (SELECT recid FROM changed)