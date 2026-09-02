
        
            delete from "iceberg"."staging"."account_attributes"
            where (
                recid) in (
                select recid
                from "iceberg"."staging"."account_attributes__dbt_tmp"
            );

        
    

    insert into "iceberg"."staging"."account_attributes" ("recid", "field_index", "m_index", "s_index", "field_value", "xml_hash", "ingested_at")
    (
        select "recid", "field_index", "m_index", "s_index", "field_value", "xml_hash", "ingested_at"
        from "iceberg"."staging"."account_attributes__dbt_tmp"
    )