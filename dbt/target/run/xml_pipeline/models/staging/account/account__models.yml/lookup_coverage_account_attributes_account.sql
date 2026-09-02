
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT DISTINCT a.field_index
FROM "iceberg"."staging"."account_attributes" AS a
LEFT JOIN "iceberg"."bronze"."lookup_metadata" AS l
  ON l.table_name = 'account' AND l.field_index = a.field_index
WHERE l.field_index IS NULL


  
  
      
    ) dbt_internal_test