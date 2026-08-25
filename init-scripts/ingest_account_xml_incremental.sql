-- Production incremental load. Run this in DBeaver's Trino editor only.
-- Oracle is read-only. c167 (YYYYMMDD) is the authoritative source-change
-- watermark. The one-day overlap makes re-runs and same-day changes safe.
--
-- Prerequisite: run ingest_account_xml_to_iceberg.sql once to bootstrap the
-- lookup and reporting tables. Schedule this script afterwards.
--
-- Important: the Oracle passthrough currently extracts all XML records, then
-- filters in Iceberg. This keeps the source untouched and makes the target
-- incremental. For a true source-side incremental scan, have the orchestrator
-- inject the stored watermark into this read-only Oracle native query.

CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3://warehouse/bronze');

ALTER TABLE iceberg.bronze.account_xml_attributes
  ADD COLUMN IF NOT EXISTS source_updated_date DATE;

DROP TABLE IF EXISTS iceberg.bronze.account_xml_attributes_stage;

CREATE TABLE iceberg.bronze.account_xml_attributes_stage
WITH (format = 'PARQUET') AS
SELECT
  recid,
  currency,
  field_index,
  TRY_CAST(NULLIF(multi_value_index, '') AS INTEGER) AS multi_value_index,
  field_value,
  TRY(CAST(date_parse(NULLIF(source_updated_text, ''), '%Y%m%d') AS DATE)) AS source_updated_date,
  current_timestamp AS ingested_at
FROM TABLE(
  oracle.system.query(
    query => 'SELECT
      a.recid,
      a.currency,
      XMLCAST(
        XMLQUERY(''/row/c167/text()'' PASSING a.xmlrecord RETURNING CONTENT)
        AS VARCHAR2(8)
      ) AS source_updated_text,
      x.field_index,
      x.multi_value_index,
      x.field_value
    FROM account a
    CROSS JOIN XMLTABLE(
      ''/row/*''
      PASSING a.xmlrecord
      COLUMNS
        field_index       VARCHAR2(16)   PATH ''name(.)'',
        multi_value_index VARCHAR2(10)   PATH ''@m'',
        field_value       VARCHAR2(4000) PATH ''text()''
    ) x'
  )
);

-- Preserve the known Unicode repair in Iceberg only; Oracle is never updated.
UPDATE iceberg.bronze.account_xml_attributes_stage
SET field_value = from_utf8(from_hex('D8B9D985D98AD98420D8AAD8ACD8B1D98AD8A8D98A20D985D8ACD987D988D984'))
WHERE field_index = 'c20'
  AND multi_value_index = 4;

UPDATE iceberg.bronze.account_xml_attributes_stage
SET field_value = from_utf8(from_hex('D8B9D985D98AD98420D8AAD8ACD8B1D98AD8A8D98A20D985D8ACD987D988D984'))
WHERE recid = '9000000112345001'
  AND field_index IN ('c3', 'c5');

DROP TABLE IF EXISTS iceberg.bronze.account_changed_records_stage;

CREATE TABLE iceberg.bronze.account_changed_records_stage
WITH (format = 'PARQUET') AS
SELECT DISTINCT source.recid
FROM iceberg.bronze.account_xml_attributes_stage AS source
CROSS JOIN (
  SELECT coalesce(date_add('day', -1, max(source_updated_date)), DATE '1900-01-01') AS lower_bound
  FROM iceberg.bronze.account_xml_attributes
) AS watermark
WHERE source.source_updated_date >= watermark.lower_bound
   OR source.source_updated_date IS NULL;

MERGE INTO iceberg.bronze.account_xml_attributes AS target
USING (
  SELECT source.*
  FROM iceberg.bronze.account_xml_attributes_stage AS source
  JOIN iceberg.bronze.account_changed_records_stage AS changed
    ON changed.recid = source.recid
) AS source
ON target.recid = source.recid
  AND target.field_index = source.field_index
  AND coalesce(target.multi_value_index, -1) = coalesce(source.multi_value_index, -1)
WHEN MATCHED AND (
  target.currency IS DISTINCT FROM source.currency
  OR target.field_value IS DISTINCT FROM source.field_value
  OR target.source_updated_date IS DISTINCT FROM source.source_updated_date
) THEN UPDATE SET
  currency = source.currency,
  field_value = source.field_value,
  source_updated_date = source.source_updated_date,
  ingested_at = source.ingested_at
WHEN NOT MATCHED THEN INSERT (
  recid, currency, field_index, multi_value_index, field_value, source_updated_date, ingested_at
) VALUES (
  source.recid, source.currency, source.field_index, source.multi_value_index, source.field_value,
  source.source_updated_date, source.ingested_at
);

-- Rebuild only changed reporting rows from the Iceberg attribute target.
-- Deletes in Oracle are intentionally not inferred: production deletion needs
-- a source tombstone/CDC feed so a temporary extraction cannot remove history.
DELETE FROM iceberg.bronze.account_flat
WHERE recid IN (SELECT recid FROM iceberg.bronze.account_changed_records_stage);

INSERT INTO iceberg.bronze.account_flat
SELECT
  recid,
  max_by(currency, ingested_at) AS currency,
  max(CASE WHEN field_index = 'c1' AND multi_value_index IS NULL THEN field_value END) AS customer_id,
  max(CASE WHEN field_index = 'c2' AND multi_value_index IS NULL THEN field_value END) AS category_code,
  max(CASE WHEN field_index = 'c3' AND multi_value_index IS NULL THEN field_value END) AS account_title,
  max(CASE WHEN field_index = 'c7' AND multi_value_index IS NULL THEN field_value END) AS mnemonic,
  max(CASE WHEN field_index = 'c8' AND multi_value_index IS NULL THEN field_value END) AS account_currency_code,
  max(CASE WHEN field_index = 'c78' AND multi_value_index IS NULL THEN field_value END) AS opening_date_text,
  max(CASE WHEN field_index = 'c85' AND multi_value_index IS NULL THEN field_value END) AS product_category_code,
  max(CASE WHEN field_index = 'c252' AND multi_value_index IS NULL THEN field_value END) AS company_code,
  max(ingested_at) AS ingested_at
FROM iceberg.bronze.account_xml_attributes
WHERE recid IN (SELECT recid FROM iceberg.bronze.account_changed_records_stage)
GROUP BY recid;

DELETE FROM iceberg.bronze.account_wide
WHERE recid IN (SELECT recid FROM iceberg.bronze.account_changed_records_stage);

INSERT INTO iceberg.bronze.account_wide
SELECT
  recid,
  max_by(currency, ingested_at) AS source_currency,
  max(CASE WHEN field_index = 'c11' AND multi_value_index IS NULL THEN field_value END) AS "account_officer",
  max(CASE WHEN field_index = 'c3' AND multi_value_index IS NULL THEN field_value END) AS "account_title_1",
  max(CASE WHEN field_index = 'c108' AND multi_value_index IS NULL THEN field_value END) AS "allow_netting",
  max(CASE WHEN field_index = 'c29' AND multi_value_index IS NULL THEN field_value END) AS "amnt_last_cr_cust",
  max(CASE WHEN field_index = 'c41' AND multi_value_index IS NULL THEN field_value END) AS "amnt_last_dr_auto",
  max(CASE WHEN field_index = 'c44' AND multi_value_index IS NULL THEN field_value END) AS "amnt_last_dr_bank",
  max(CASE WHEN field_index = 'c2' AND multi_value_index IS NULL THEN field_value END) AS "category",
  max(CASE WHEN field_index = 'c93' AND multi_value_index IS NULL THEN field_value END) AS "charge_ccy",
  max(CASE WHEN field_index = 'c248' AND multi_value_index IS NULL THEN field_value END) AS "curr_no",
  max(CASE WHEN field_index = 'c8' AND multi_value_index IS NULL THEN field_value END) AS "currency",
  max(CASE WHEN field_index = 'c31' AND multi_value_index IS NULL THEN field_value END) AS "date_last_cr_auto",
  max(CASE WHEN field_index = 'c43' AND multi_value_index IS NULL THEN field_value END) AS "date_last_dr_bank",
  max(CASE WHEN field_index = 'c37' AND multi_value_index IS NULL THEN field_value END) AS "date_last_dr_cust",
  max(CASE WHEN field_index = 'c167' AND multi_value_index IS NULL THEN field_value END) AS "date_last_update",
  max(CASE WHEN field_index = 'c96' AND multi_value_index IS NULL THEN field_value END) AS "interest_mkt",
  max(CASE WHEN field_index = 'c23' AND multi_value_index IS NULL THEN field_value END) AS "open_actual_bal",
  max(CASE WHEN field_index = 'c78' AND multi_value_index IS NULL THEN field_value END) AS "opening_date",
  max(CASE WHEN field_index = 'c76' AND multi_value_index IS NULL THEN field_value END) AS "passbook",
  max(CASE WHEN field_index = 'c7' AND multi_value_index IS NULL THEN field_value END) AS "position_type",
  max(CASE WHEN field_index = 'c5' AND multi_value_index IS NULL THEN field_value END) AS "short_title",
  max(CASE WHEN field_index = 'c77' AND multi_value_index IS NULL THEN field_value END) AS "start_year_bal",
  max(CASE WHEN field_index = 'c33' AND multi_value_index IS NULL THEN field_value END) AS "tran_last_cr_auto",
  max(CASE WHEN field_index = 'c36' AND multi_value_index IS NULL THEN field_value END) AS "tran_last_cr_bank",
  max(CASE WHEN field_index = 'c30' AND multi_value_index IS NULL THEN field_value END) AS "tran_last_cr_cust",
  max(CASE WHEN field_index = 'c42' AND multi_value_index IS NULL THEN field_value END) AS "tran_last_dr_auto",
  max(CASE WHEN field_index = 'c45' AND multi_value_index IS NULL THEN field_value END) AS "tran_last_dr_bank",
  max(CASE WHEN field_index = 'c39' AND multi_value_index IS NULL THEN field_value END) AS "tran_last_dr_cust",
  max(CASE WHEN field_index = 'c32' AND multi_value_index IS NULL THEN field_value END) AS "amnt_last_cr_auto",
  max(CASE WHEN field_index = 'c35' AND multi_value_index IS NULL THEN field_value END) AS "amnt_last_cr_bank",
  max(CASE WHEN field_index = 'c38' AND multi_value_index IS NULL THEN field_value END) AS "amnt_last_dr_cust",
  max(CASE WHEN field_index = 'c251' AND multi_value_index IS NULL THEN field_value END) AS "authoriser",
  max(CASE WHEN field_index = 'c94' AND multi_value_index IS NULL THEN field_value END) AS "charge_mkt",
  max(CASE WHEN field_index = 'c252' AND multi_value_index IS NULL THEN field_value END) AS "co_code",
  max(CASE WHEN field_index = 'c21' AND multi_value_index IS NULL THEN field_value END) AS "condition_group",
  max(CASE WHEN field_index = 'c9' AND multi_value_index IS NULL THEN field_value END) AS "currency_market",
  max(CASE WHEN field_index = 'c1' AND multi_value_index IS NULL THEN field_value END) AS "customer",
  max(CASE WHEN field_index = 'c34' AND multi_value_index IS NULL THEN field_value END) AS "date_last_cr_bank",
  max(CASE WHEN field_index = 'c28' AND multi_value_index IS NULL THEN field_value END) AS "date_last_cr_cust",
  max(CASE WHEN field_index = 'c40' AND multi_value_index IS NULL THEN field_value END) AS "date_last_dr_auto",
  max(CASE WHEN field_index = 'c253' AND multi_value_index IS NULL THEN field_value END) AS "dept_code",
  max(CASE WHEN field_index = 'c121' AND multi_value_index IS NULL THEN field_value END) AS "from_date",
  max(CASE WHEN field_index = 'c141' AND multi_value_index IS NULL THEN field_value END) AS "hvt_flag",
  max(CASE WHEN field_index = 'c95' AND multi_value_index IS NULL THEN field_value END) AS "interest_ccy",
  max(CASE WHEN field_index = 'c122' AND multi_value_index IS NULL THEN field_value END) AS "locked_amount",
  max(CASE WHEN field_index = 'c25' AND multi_value_index IS NULL THEN field_value END) AS "online_actual_bal",
  max(CASE WHEN field_index = 'c26' AND multi_value_index IS NULL THEN field_value END) AS "online_cleared_bal",
  max(CASE WHEN field_index = 'c149' AND multi_value_index IS NULL THEN field_value END) AS "open_available_bal",
  max(CASE WHEN field_index = 'c85' AND multi_value_index IS NULL THEN field_value END) AS "open_category",
  max(CASE WHEN field_index = 'c24' AND multi_value_index IS NULL THEN field_value END) AS "open_cleared_bal",
  max(CASE WHEN field_index = 'c27' AND multi_value_index IS NULL THEN field_value END) AS "working_balance",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c20') AS "c20_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c46') AS "cap_date_charge_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c47') AS "cap_date_cr_int_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c48') AS "cap_date_c2_int_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c49') AS "cap_date_dr_int_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c50') AS "cap_date_d2_int_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c99') AS "alt_acct_type_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c100') AS "alt_acct_id_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c249') AS "inputter_values",
  array_agg(field_value ORDER BY coalesce(multi_value_index, 0)) FILTER (WHERE field_index = 'c250') AS "date_time_values",
  max(ingested_at) AS ingested_at
FROM iceberg.bronze.account_xml_attributes
WHERE recid IN (SELECT recid FROM iceberg.bronze.account_changed_records_stage)
GROUP BY recid;

SELECT
  (SELECT count(*) FROM iceberg.bronze.account_changed_records_stage) AS changed_account_count,
  (SELECT max(source_updated_date) FROM iceberg.bronze.account_xml_attributes) AS loaded_through;
