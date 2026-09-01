{#
  Custom incremental strategy: config(incremental_strategy='conditional_merge').

  dbt-trino's built-in `merge` strategy always emits an unconditional
  WHEN MATCHED THEN UPDATE. That's wrong here: account_attributes windows
  its own re-tokenization by account_raw.ingested_at, so an unconditional
  update would bump ingested_at on every unchanged row and force a full
  re-tokenize downstream on every run. This is a 1:1 port of gen_sql.py's
  mode_ingest_refresh MERGE (see gen_sql.py:251-258) -- only rows whose
  xmlrecord actually changed get ingested_at bumped.

  dbt-core looks this macro up by exact name get_incremental_<strategy>_sql
  (adapters/base/impl.py: get_incremental_strategy_macro) -- no adapter
  prefix needed for a project-local custom strategy.
#}
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
