{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='recid'
  )
}}

{{ raw_ingest_model() }}
