
{% macro get_incremental_conditional_merge_sql(arg_dict) %}
  {%- set target = arg_dict["target_relation"] -%}
  {%- set source = arg_dict["temp_relation"] -%}
  {%- set dest_columns = arg_dict["dest_columns"] -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  merge into {{ target }} as t
  using {{ source }} as s
  on t.recid = s.recid
  when matched and t.xmlrecord is distinct from s.xmlrecord then update set
    xmlrecord = s.xmlrecord,
    ingested_at = s.ingested_at
  when not matched then insert ({{ dest_cols_csv }})
  values (s.recid, s.xmlrecord, s.ingested_at)
{% endmacro %}
