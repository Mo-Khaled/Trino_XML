{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='recid',
    properties={'partitioning': "ARRAY['field_index']"}
  )
}}

{{ attributes_model() }}
