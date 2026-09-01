
        
            delete from "iceberg"."staging"."account_raw"
            where (
                recid) in (
                select recid
                from "iceberg"."staging"."account_raw__dbt_tmp"
            );

        
    

    insert into "iceberg"."staging"."account_raw" ("recid", "xmlrecord", "ingested_at")
    (
        select "recid", "xmlrecord", "ingested_at"
        from "iceberg"."staging"."account_raw__dbt_tmp"
    )