
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select recid
from "iceberg"."staging"."account_raw"
where recid is null



  
  
      
    ) dbt_internal_test