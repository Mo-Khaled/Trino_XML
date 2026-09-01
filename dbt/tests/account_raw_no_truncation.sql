{#
  Ported from gen_sql.py's mode_ingest truncation check: compares Iceberg's
  landed length against Oracle's dbms_lob.getlength() for every row. Any row
  returned here means getClobVal() truncated something -- a dbt test failure
  instead of an eyeballed SELECT in DBeaver.
#}
WITH iceberg_lengths AS (
  SELECT recid, length(xmlrecord) AS iceberg_length
  FROM {{ ref('account_raw') }}
),
oracle_lengths AS (
  SELECT recid, oracle_length
  FROM TABLE(
    oracle.system.query(
      query => 'SELECT a.recid, CAST(dbms_lob.getlength(a.xmlrecord.getclobval()) AS NUMBER(10)) AS oracle_length
      FROM {{ var("oracle_schema") }}.account a'
    )
  )
)
SELECT i.recid, i.iceberg_length, o.oracle_length
FROM iceberg_lengths i
JOIN oracle_lengths o ON i.recid = o.recid
WHERE i.iceberg_length <> o.oracle_length
