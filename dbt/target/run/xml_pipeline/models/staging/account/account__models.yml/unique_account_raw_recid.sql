
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    recid as unique_field,
    count(*) as n_records

from "iceberg"."staging"."account_raw"
where recid is not null
group by recid
having count(*) > 1



  
  
      
    ) dbt_internal_test