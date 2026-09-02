
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
WITH iceberg_lengths AS (
  SELECT recid, length(xmlrecord) AS iceberg_length
  FROM "iceberg"."staging"."account_raw"
),
oracle_lengths AS (
  SELECT recid, oracle_length
  FROM TABLE(
    oracle.system.query(
      query => 'SELECT a.recid, CAST(dbms_lob.getlength(a.xmlrecord.getclobval()) AS NUMBER(10)) AS oracle_length
      FROM source_user.account a'
    )
  )
)
SELECT i.recid, i.iceberg_length, o.oracle_length
FROM iceberg_lengths i
JOIN oracle_lengths o ON i.recid = o.recid
WHERE i.iceberg_length <> o.oracle_length
  
  
      
    ) dbt_internal_test