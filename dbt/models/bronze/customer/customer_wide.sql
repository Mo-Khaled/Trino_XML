{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='recid',
    on_schema_change='append_new_columns'
  )
}}

{{ wide_model() }}
