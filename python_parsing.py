    from pyspark.sql import DataFrame
    from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType
    from pyspark.sql.functions import expr
    from pyspark.sql import Column, SparkSession
    from pyspark.sql import functions as F  # noqa: N812

    from scb.core.logger import get_logger

    logger = get_logger(__name__)


    def parse_xml_with_schema(
        spark: SparkSession, xml_column: Column, schema: StructType, options: dict
    ) -> Column:
        """
        Parses an XML column in a Spark DataFrame using the Databricks XML library via JVM interop.
        Args:
            spark (SparkSession): The active Spark session.
            xml_column (Column): The column containing XML data.
            schema (StructType): The schema to apply to the parsed XML.
            options (dict): Additional options for XML parsing.
        Returns:
            Column: A Spark Column with parsed XML data.
        """
        java_schema = spark._jsparkSession.parseDataType(schema.json())
        scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
        jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
            xml_column._jc, java_schema, scala_map
        )

        return Column(jc)


    def _build_xml_schema(schema_rows: list) -> StructType:
        """
        Build the XML StructType schema (array of _VALUE / _m / _s structs)
        for every unique field_index found in schema_rows.
        """
        xml_col = []
        for row in schema_rows:
            if not any(f.name == row["field_index"] for f in xml_col):
                nested_col = [
                    StructField("_VALUE", StringType(), True),
                    StructField("_m", LongType(), True),
                    StructField("_s", LongType(), True),
                ]
                xml_col.append(
                    StructField(row["field_index"], ArrayType(StructType(nested_col)), True)
                )
        return StructType(xml_col)


    def _detect_s_value_fields(
        df_parsed: DataFrame, schema_rows: list, xml_col_name: str = "XMLRECORD"
    ) -> set:
        """
        Pre-scan the already-parsed DataFrame to discover which fields (those whose
        mapping row has no m_index) actually carry s sub-values (s > 1) in the data.

        Returns a set of field_index strings, e.g. {'c28', 'c29'}.

        WHY this pre-scan is required
        ──────────────────────────────
        A Spark SQL CASE WHEN must return the *same type* from every branch — the
        type is resolved at analysis time, not per-row.  If we tried to emit
        ARRAY<ARRAY<STRING>> in the s-present branch and ARRAY<STRING> in the
        s-absent branch inside a single expression we would get an AnalysisException.

        By scanning first we know — at Python build time — which fields need
        ARRAY<ARRAY<STRING>> vs ARRAY<STRING>, and we generate a single-type
        expression for each field.
        """
        else_fields = [r["field_index"] for r in schema_rows if r["m_index"] is None]
        if not else_fields:
            return set()

        check_exprs = [
            F.max(
                F.when(
                    F.col(f"{xml_col_name}_parsed.{f}").isNotNull()
                    & (
                        F.expr(
                            f"size(filter({xml_col_name}_parsed.{f},"
                            f" x -> cast(coalesce(x._s, 1) as int) > 1))"
                        )
                        > 0
                    ),
                    F.lit(True),
                ).otherwise(F.lit(False))
            ).alias(f)
            for f in else_fields
        ]
        scan_result = df_parsed.select(*check_exprs).collect()[0].asDict()
        return {f for f, has_s in scan_result.items() if has_s}


    def _build_select_expressions(
        schema_rows: list, fields_with_s: set, xml_col_name: str = "XMLRECORD"
    ) -> list:
        """
        Produce one Spark Column expression per mapping row.

        Three cases, determined at Python build time so each expression has a
        single, consistent return type:

        Branch 1 – m_index is not None
            Fixed m group, iterate over s slots → ARRAY<STRING>

        Branch 2 – m_index is None AND field in fields_with_s
            Iterate over m groups then s slots → ARRAY<ARRAY<STRING>>
            Example (c28 with 4 m-groups × 3 s-slots):
                [['IN','PR','PE'], ['IN','PR','PE'], ['IN','PR','PE'], ['IN','PR','PE']]
            Example (c28 with 1 m-group × 3 s-slots):
                [['IN','PR','PE']]

        Branch 3 – m_index is None AND field NOT in fields_with_s
            Iterate over m groups only → ARRAY<STRING>
        """
        selected_columns = []
        name = xml_col_name

        for column_a in schema_rows:
            col_expr = f"{name}_parsed.{column_a['field_index']}"
            field = column_a["field_index"]

            if column_a["m_index"] is not None:
                # ── Branch 1: fixed m group, iterate s ────────────────────────────
                m_idx = column_a["m_index"]
                fe = f"filter({col_expr}, x -> cast(coalesce(x._m, 1) as int) == {m_idx})"
                transformed_expr = (
                    f"CASE WHEN {col_expr} IS NULL THEN NULL "
                    f"WHEN size({fe}) = 0 THEN CAST(array() AS ARRAY<STRING>) "
                    f"ELSE transform("
                    f"  sequence(1, cast(coalesce(array_max(transform({fe}, x -> cast(coalesce(x._s, 1) as int))), 1) as int)), "  # noqa: E501
                    f"  i -> CASE WHEN size(filter({fe}, x -> cast(coalesce(x._s, 1) as int) == i)) > 0 "
                    f"       THEN element_at(filter({fe}, x -> cast(coalesce(x._s, 1) as int) == i), 1)._VALUE "
                    f"       ELSE NULL END"
                    f") END"
                )

            elif field in fields_with_s:
                # ── Branch 2: no m_index, has s values → ARRAY<ARRAY<STRING>> ────
                # Outer transform iterates m groups (coalesce NULL→1).
                # Inner transform iterates s slots within each m group.
                # The nested lambda captures the outer variable `i` — valid Spark SQL.
                # coalesce(_m,1) and coalesce(_s,1) ensure elements with no
                # m or s attribute are treated as m=1 / s=1 respectively.
                transformed_expr = (
                    f"CASE WHEN {col_expr} IS NULL THEN NULL "
                    f"WHEN size({col_expr}) = 0 THEN CAST(array() AS ARRAY<ARRAY<STRING>>) "
                    f"ELSE transform("
                    f"  sequence(1, cast(coalesce(array_max(transform({col_expr}, x -> cast(coalesce(x._m, 1) as int))), 1) as int)), "  # noqa: E501
                    f"  i -> transform("
                    f"    sequence(1, cast(coalesce(array_max(transform("
                    f"      filter({col_expr}, x -> cast(coalesce(x._m, 1) as int) == i),"
                    f"      x -> cast(coalesce(x._s, 1) as int))), 1) as int)), "
                    f"    j -> CASE"
                    f"      WHEN size(filter({col_expr},"
                    f"           x -> cast(coalesce(x._m, 1) as int) == i"
                    f"             AND cast(coalesce(x._s, 1) as int) == j)) > 0"
                    f"      THEN element_at(filter({col_expr},"
                    f"           x -> cast(coalesce(x._m, 1) as int) == i"
                    f"             AND cast(coalesce(x._s, 1) as int) == j), 1)._VALUE"
                    f"      ELSE NULL END"
                    f"  )"
                    f") END"
                )

            else:
                # ── Branch 3: no m_index, no s values → flat ARRAY<STRING> ───────
                transformed_expr = (
                    f"CASE WHEN {col_expr} IS NULL THEN NULL "
                    f"WHEN size({col_expr}) = 0 THEN CAST(array() AS ARRAY<STRING>) "
                    f"ELSE transform("
                    f"  sequence(1, cast(coalesce(array_max(transform({col_expr}, x -> cast(coalesce(x._m, 1) as int))), 1) as int)), "  # noqa: E501
                    f"  i -> CASE WHEN size(filter({col_expr}, x -> cast(coalesce(x._m, 1) as int) == i)) > 0 "
                    f"       THEN element_at(filter({col_expr}, x -> cast(coalesce(x._m, 1) as int) == i), 1)._VALUE "
                    f"       ELSE NULL END"
                    f") END"
                )

            selected_columns.append(
                expr(transformed_expr).alias(column_a["resolved_name_en"])
            )

        return selected_columns


    def build_xml_transformation_config(schema: DataFrame) -> tuple[list, dict]:
        """
        Extracts selected columns and XML schemas from the schema configuration.

        Args:
            schema (DataFrame): Schema configuration DataFrame containing column definitions.

        Returns:
            tuple: (selected_columns, xml_schemas) where selected_columns is always an empty
                list (expressions are now built inside apply_xml_parsing after the s-value
                pre-scan) and xml_schemas is a dict mapping the XML column name to its
                StructType schema.

        Note:
            Column expressions are built by _build_select_expressions() inside
            apply_xml_parsing(), which first runs _detect_s_value_fields() to
            determine per-field output types at Python build time.
        """
        rows = schema.collect()
        xml_schema = _build_xml_schema(rows)
        return [], {"XMLRECORD": xml_schema}


    def normalize_arrays(df):
        """
        Flatten single-element ARRAY<STRING> columns to scalars.

        ARRAY<ARRAY<STRING>> columns are NEVER flattened — their two-level structure
        (outer = m groups, inner = s slots) is semantically meaningful. Collapsing
        them with ELEMENT_AT would destroy the grouping and produce incorrect data
        when reconcile_iceberg_schema later tries to re-wrap the values.
        """
        # Use isinstance instead of string-prefix check for correctness
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not array_cols:
            # Nothing to do — return df unchanged to avoid an undefined select_exprs reference
            return df

        # Compute max outer-array size for each array column in one pass
        size_info = (
            df.select(*[F.max(F.size(col)).alias(col) for col in array_cols]).collect()[0].asDict()
        )
        select_exprs = []
        for col in df.columns:
            if col in array_cols:
                col_type = df.schema[col].dataType
                is_aoa = isinstance(col_type, ArrayType) and isinstance(
                    col_type.elementType, ArrayType
                )
                if is_aoa:
                    # ARRAY<ARRAY<STRING>>: always keep — never flatten
                    select_exprs.append(col)
                elif size_info[col] and size_info[col] > 1:
                    # Multi-element flat array: keep as-is
                    select_exprs.append(col)
                else:
                    # Single-element flat array: extract scalar
                    select_exprs.append(F.expr(f"ELEMENT_AT({col}, 1)").alias(col))
            else:
                select_exprs.append(F.col(col))
        # Apply the transformations to the DataFrame
        return df.select(*select_exprs)



    def apply_xml_parsing(spark, df, schema, metadata_cols):
        logger.info("Building XML schema configuration")
        rows = schema.collect()
        xml_col_name = "XMLRECORD"
        xml_schema = _build_xml_schema(rows)

        # Pass 1: parse the raw XML column into structured arrays
        logger.info(f"Applying XML parsing for column: {xml_col_name}")
        df_parsed = df.withColumn(
            f"{xml_col_name}_parsed",
            parse_xml_with_schema(spark, F.col(xml_col_name), xml_schema, {"rowTag": "row"}),
        )

        # Pass 2: determine which no-m_index fields carry s sub-values so we can
        # emit the correct return type (ARRAY<ARRAY<STRING>> vs ARRAY<STRING>)
        # at expression-build time — not inside a runtime CASE WHEN.
        logger.info("Pre-scanning for fields with s sub-values...")
        fields_with_s = _detect_s_value_fields(df_parsed, rows, xml_col_name)
        if fields_with_s:
            logger.info(f"Detected s sub-value fields: {sorted(fields_with_s)}")
        else:
            logger.info("No s sub-value fields detected.")

        # Build typed SELECT expressions and finalise the DataFrame
        logger.info("Building SELECT expressions...")
        selected_columns = _build_select_expressions(rows, fields_with_s, xml_col_name)

        df = df_parsed.select(*selected_columns, *metadata_cols)
        return df


    def _is_array_of_array(data_type) -> bool:
        """Return True if data_type is ARRAY<ARRAY<...>> (two levels of ArrayType)."""
        return isinstance(data_type, ArrayType) and isinstance(data_type.elementType, ArrayType)


    def _migrate_table_column(
        spark, iceberg_table, col_name, new_type_sql, table_field_map, df, update_expr=None
    ):
        """
        Generic helper: adds a new typed column to the Iceberg table, copies the old
        column's data into it, drops the old column, and renames the new one.

        Used when the table has a narrower type (STRING or ARRAY<STRING>) and the
        DataFrame now produces a wider type (ARRAY<STRING> or ARRAY<ARRAY<STRING>>).

        Args:
            spark:            Active SparkSession.
            iceberg_table:    Fully-qualified Iceberg table name.
            col_name:         The column being migrated.
            new_type_sql:     SQL type string for the new column, e.g. 'ARRAY<STRING>'.
            table_field_map:  Mutable dict of {col_name: DataType} — updated in place.
            df:               Current DataFrame — returned with any temp column removed.
            update_expr:      SQL expression used to populate the new column from the old
                            one.  Defaults to ``array({col_name})`` (wraps one level).
                            Pass ``array(array({col_name}))`` when migrating a STRING
                            column all the way to ARRAY<ARRAY<STRING>>.

        Returns:
            Updated DataFrame.
        """
        if update_expr is None:
            update_expr = f"array({col_name})"

        tmp = f"{col_name}_array"
        try:
            if tmp not in table_field_map:
                spark.sql(
                    f"ALTER TABLE {iceberg_table} ADD COLUMN {tmp} {new_type_sql}"
                )
            else:
                logger.warning(
                    f"Temporary column '{tmp}' already exists from a previous run. "
                    "Resuming conversion..."
                )

            spark.sql(  # noqa: S608
                f"UPDATE {iceberg_table} SET {tmp} = {update_expr}"
            )
            spark.sql(f"ALTER TABLE {iceberg_table} DROP COLUMN {col_name}")
            spark.sql(f"ALTER TABLE {iceberg_table} RENAME COLUMN {tmp} TO {col_name}")

            # table_field_map was built before the migration — keep it consistent
            table_field_map[col_name] = spark.table(iceberg_table).schema[col_name].dataType

            # The missing-column guard may have added a null temp column to df
            if tmp in df.columns:
                df = df.drop(tmp)

        except Exception as e:
            logger.error(f"Failed to migrate column '{col_name}' to {new_type_sql}: {e}")

        return df


    def reconcile_iceberg_schema(spark, df, iceberg_table):
        logger.info(
            "Normalizing column types (STRING / ARRAY<STRING> / ARRAY<ARRAY<STRING>>) "
            "between DataFrame and Iceberg schema"
        )
        table_schema = spark.table(iceberg_table).schema

        df_field_map = {f.name: f.dataType for f in df.schema.fields}
        table_field_map = {f.name: f.dataType for f in table_schema.fields}

        # ── Step 1: add columns missing from the DataFrame ────────────────────────
        for col_name, table_type in table_field_map.items():
            if col_name not in df_field_map:
                logger.warning(
                    f"Column '{col_name}' exists in table but not in DataFrame. Adding as null..."
                )
                if isinstance(table_type, ArrayType):
                    df = df.withColumn(col_name, F.array().cast(table_type))
                else:
                    df = df.withColumn(col_name, F.lit(None).cast(table_type))
                df_field_map[col_name] = table_type

        # ── Step 2: add columns missing from the Iceberg table ───────────────────
        for col_name, df_type in df_field_map.items():
            if col_name not in table_field_map:
                logger.warning(
                    f"Column '{col_name}' exists in DataFrame but not in table. "
                    "Altering table to add column..."
                )
                col_type_sql = df_type.simpleString().upper()
                alter_stmt = f"ALTER TABLE {iceberg_table} ADD COLUMN {col_name} {col_type_sql}"
                try:
                    spark.sql(alter_stmt)
                    logger.info(
                        f"Added column '{col_name}' with type {col_type_sql} to {iceberg_table}"
                    )
                    table_field_map[col_name] = df_type
                except Exception as e:
                    logger.error(f"Failed to add column '{col_name}' to table: {e}")

        # ── Step 3: resolve type mismatches ──────────────────────────────────────
        for col_name, df_type in df_field_map.items():
            if col_name not in table_field_map:
                continue
            table_type = table_field_map[col_name]

            df_is_aoa = _is_array_of_array(df_type)           # ARRAY<ARRAY<...>>
            df_is_arr = isinstance(df_type, ArrayType) and not df_is_aoa  # ARRAY<STRING>
            df_is_str = isinstance(df_type, StringType)

            tbl_is_aoa = _is_array_of_array(table_type)
            tbl_is_arr = isinstance(table_type, ArrayType) and not tbl_is_aoa
            tbl_is_str = isinstance(table_type, StringType)

            # ── TABLE MIGRATION cases (table is narrower than DataFrame) ──────────

            # Case A: DataFrame ARRAY<ARRAY<STRING>>, table STRING
            #         Migrate: STRING → ARRAY<ARRAY<STRING>>
            if df_is_aoa and tbl_is_str:
                logger.info(
                    f"Column '{col_name}': DataFrame is ARRAY<ARRAY<STRING>>, table is STRING. "
                    "Migrating table column to ARRAY<ARRAY<STRING>>..."
                )
                # STRING → ARRAY<ARRAY<STRING>>: need two levels of wrapping
                df = _migrate_table_column(
                    spark, iceberg_table, col_name,
                    "ARRAY<ARRAY<STRING>>", table_field_map, df,
                    update_expr=f"array(array({col_name}))",
                )

            # Case B: DataFrame ARRAY<ARRAY<STRING>>, table ARRAY<STRING>
            #         Migrate: ARRAY<STRING> → ARRAY<ARRAY<STRING>>
            elif df_is_aoa and tbl_is_arr:
                logger.info(
                    f"Column '{col_name}': DataFrame is ARRAY<ARRAY<STRING>>, table is ARRAY<STRING>. "
                    "Migrating table column to ARRAY<ARRAY<STRING>>..."
                )
                df = _migrate_table_column(
                    spark, iceberg_table, col_name,
                    "ARRAY<ARRAY<STRING>>", table_field_map, df
                )

            # Case C: DataFrame ARRAY<STRING>, table STRING  (original Case 1)
            #         Migrate: STRING → ARRAY<STRING>
            elif df_is_arr and tbl_is_str:
                logger.info(
                    f"Column '{col_name}': DataFrame is ARRAY<STRING>, table is STRING. "
                    "Migrating table column to ARRAY<STRING>..."
                )
                df = _migrate_table_column(
                    spark, iceberg_table, col_name,
                    "ARRAY<STRING>", table_field_map, df
                )

            # ── DATAFRAME CAST cases (table is wider than DataFrame) ──────────────

            # Case D: DataFrame STRING, table ARRAY<ARRAY<STRING>>
            #         Wrap: string → array(array(val))
            elif df_is_str and tbl_is_aoa:
                logger.info(
                    f"Column '{col_name}': DataFrame is STRING, table is ARRAY<ARRAY<STRING>>. "
                    "Wrapping DataFrame column in array(array(...))..."
                )
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNotNull(),
                        F.array(F.array(F.col(col_name))),
                    ).otherwise(F.lit(None).cast(table_type)),
                )

            # Case E: DataFrame ARRAY<STRING>, table ARRAY<ARRAY<STRING>>
            #         Wrap each element: transform(col, x -> array(x))
            elif df_is_arr and tbl_is_aoa:
                logger.info(
                    f"Column '{col_name}': DataFrame is ARRAY<STRING>, table is ARRAY<ARRAY<STRING>>. "
                    "Wrapping each element in array(x) on DataFrame..."
                )
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNotNull(),
                        F.expr(f"transform({col_name}, x -> array(x))"),
                    ).otherwise(F.lit(None).cast(table_type)),
                )

            # Case F: DataFrame STRING, table ARRAY<STRING>  (original Case 2)
            #         Wrap: string → array(val)
            elif df_is_str and tbl_is_arr:
                logger.info(
                    f"Column '{col_name}': DataFrame is STRING, table is ARRAY<STRING>. "
                    "Wrapping DataFrame column in array(...)..."
                )
                df = df.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNotNull(), F.array(F.col(col_name))).otherwise(
                        F.array().cast(table_type)
                    ),
                )

        return df