


WITH grouped AS (
  SELECT
    recid,
    map_agg(field_index || '_' || CAST(m_index AS VARCHAR), field_value) AS f,
    multimap_agg(
      field_index || '_' || CAST(m_index AS VARCHAR),
      CAST(ROW(s_index, field_value) AS ROW(s INTEGER, val VARCHAR))
    ) AS g,
    max(xml_hash) AS xml_hash
  FROM "iceberg"."staging"."customer_attributes"
  GROUP BY recid
)
SELECT
  recid,
  TRY(CAST(date_parse(element_at(f, 'c167_1'), '%Y%m%d') AS DATE)) AS source_updated_date,
  xml_hash,
  current_timestamp AS ingested_at
FROM grouped