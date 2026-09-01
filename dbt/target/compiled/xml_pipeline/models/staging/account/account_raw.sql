

SELECT
  recid,
  xmlrecord,
  current_timestamp AS ingested_at
FROM TABLE(
  oracle.system.query(
    query => 'SELECT
      a.recid,
      a.xmlrecord.getClobVal() AS xmlrecord
    FROM source_user.account a'
  )
)