

{% macro raw_ingest_model() %}

{%- set suffix = '_raw' -%}
{%- set table_name = model.name[:-(suffix | length)] -%}
{%- set watermark_field = var('watermark_field', 'c167') -%}
{%- set start_date = var('start_date', none) -%}
{%- set end_date = var('end_date', none) -%}
{%- if (start_date is not none) != (end_date is not none) -%}
  {{ exceptions.raise_compiler_error("start_date and end_date vars must be given together") }}
{%- endif -%}

{%- set where_clause -%}
{%- if start_date -%}
    WHERE XMLCAST(
      XMLQUERY(''/row/{{ watermark_field }}/text()'' PASSING a.xmlrecord RETURNING CONTENT)
      AS VARCHAR2(8)
    ) BETWEEN ''{{ start_date }}'' AND ''{{ end_date }}''
{%- endif -%}
{%- endset -%}

SELECT
  recid,
  xmlrecord,
  current_timestamp AS ingested_at
FROM TABLE(
  oracle.system.query(
    query => 'SELECT
      a.recid,
      a.xmlrecord.getClobVal() AS xmlrecord
    FROM {{ var("oracle_schema") }}.{{ table_name }} a{{ where_clause }}'
  )
)
{%- endmacro %}


{% macro attributes_model() %}
{%- set suffix = '_attributes' -%}
{%- set table_name = model.name[:-(suffix | length)] -%}
{%- set start_date = var('start_date', none) -%}
{%- set end_date = var('end_date', none) -%}
{%- if (start_date is not none) != (end_date is not none) -%}
  {{ exceptions.raise_compiler_error("start_date and end_date vars must be given together") }}
{%- endif -%}


{%- set target_exists = is_incremental() -%}

{%- set raw_relation = ref(table_name ~ '_raw') -%}
{%- if start_date -%}
  {%- set source_relation -%}
    (SELECT * FROM {{ raw_relation }}
     WHERE ingested_at >= date_parse('{{ start_date }}', '%Y%m%d')
     AND ingested_at < date_parse('{{ end_date }}', '%Y%m%d') + INTERVAL '1' DAY)
    AS windowed_raw
  {%- endset -%}
{%- else -%}
  {%- set source_relation = raw_relation -%}
{%- endif -%}

{{ token_ctes(source_relation) }}
{%- if target_exists %}
,
changed AS (
  SELECT DISTINCT e.recid
  FROM exploded e
  LEFT JOIN (SELECT DISTINCT recid, xml_hash FROM {{ this }}) t
    ON t.recid = e.recid
  WHERE t.recid IS NULL OR t.xml_hash IS DISTINCT FROM e.xml_hash
)
{%- endif %}
SELECT
  recid,
  tag AS field_index,
  m AS m_index,
  s AS s_index,
  val AS field_value,
  xml_hash,
  current_timestamp AS ingested_at
FROM exploded
WHERE tag != '__empty__'
{%- if target_exists %}
  AND recid IN (SELECT recid FROM changed)
{%- endif %}
{%- endmacro %}


{% macro wide_model() %}
{%- set suffix = '_wide' -%}
{%- set table_name = model.name[:-(suffix | length)] -%}
{%- set watermark_field = var('watermark_field', 'c167') -%}
{%- set start_date = var('start_date', none) -%}
{%- set end_date = var('end_date', none) -%}
{%- if (start_date is not none) != (end_date is not none) -%}
  {{ exceptions.raise_compiler_error("start_date and end_date vars must be given together") }}
{%- endif -%}

{%- set attrs_relation = ref(table_name ~ '_attributes') -%}
{%- set lookup_rows = get_lookup_rows(table_name) -%}
{%- set array_cols = get_array_columns(lookup_rows, attrs_relation, this) -%}
{%- set select_result = get_wide_select(lookup_rows, array_cols, watermark_field) -%}
{%- set columns_sql = select_result[0] -%}

{%- set target_exists = is_incremental() -%}

{%- if target_exists and start_date -%}
  {%- set source_relation -%}
    (SELECT * FROM {{ attrs_relation }}
     WHERE ingested_at >= date_parse('{{ start_date }}', '%Y%m%d')
     AND ingested_at < date_parse('{{ end_date }}', '%Y%m%d') + INTERVAL '1' DAY)
    AS windowed_attributes
  {%- endset -%}
{%- else -%}
  {%- set source_relation = attrs_relation -%}
{%- endif -%}

{{ wide_pivot_cte(source_relation) }}
{%- if target_exists %}
,
changed AS (
  SELECT DISTINCT g.recid
  FROM grouped g
  LEFT JOIN (SELECT DISTINCT recid, xml_hash FROM {{ this }}) t
    ON t.recid = g.recid
  WHERE t.recid IS NULL OR t.xml_hash IS DISTINCT FROM g.xml_hash
)
{%- endif %}
SELECT
{{ columns_sql }},
  xml_hash,
  current_timestamp AS ingested_at
FROM grouped
{%- if target_exists %}
WHERE recid IN (SELECT recid FROM changed)
{%- endif %}
{%- endmacro %}
