
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
SELECT
  count(*) AS records,
  sum(cardinality(regexp_extract_all(
    regexp_replace(xmlrecord, '<(c\d+)([^>]*)/>', '<$1$2></$1>'),
    '<(c\d+)([^>]*)>([^<]*)</c\d+>', 1
  ))) AS matched_tags,
  sum(cardinality(regexp_extract_all(xmlrecord, '<c\d+[ />]'))) AS raw_tag_opens
FROM "iceberg"."staging"."account_raw"
HAVING sum(cardinality(regexp_extract_all(
    regexp_replace(xmlrecord, '<(c\d+)([^>]*)/>', '<$1$2></$1>'),
    '<(c\d+)([^>]*)>([^<]*)</c\d+>', 1
  ))) != sum(cardinality(regexp_extract_all(xmlrecord, '<c\d+[ />]')))
  
  
      
    ) dbt_internal_test