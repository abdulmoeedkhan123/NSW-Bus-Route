"""Initial CSV-to-Lakehouse pipeline for NSW Bus Network Intelligence.

The pipeline deliberately preserves source rows. Exact duplicates and data-quality
issues are flagged for review rather than silently removed or repaired.
"""

from __future__ import annotations

import re
import uuid
from functools import reduce
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import (
    DATE_COLUMNS,
    DEFAULT_BRONZE_SCHEMA,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_SILVER_SCHEMA,
    EXPECTED_SOURCE_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    STRING_COLUMNS,
)


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str) -> str:
    """Validate a catalog/schema identifier before using it in SQL statements."""
    if not value or not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid {label} '{value}'. Use letters, numbers and underscores; "
            "the first character must be a letter or underscore."
        )
    return value


def normalize_column_name(name: str) -> str:
    """Convert CSV headers to lower snake_case, including headers with spaces."""
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return cleaned


def normalize_headers(df: DataFrame) -> DataFrame:
    """Normalize headers and fail clearly if two source columns collide."""
    normalized = [normalize_column_name(c) for c in df.columns]
    if len(normalized) != len(set(normalized)):
        duplicates = sorted({c for c in normalized if normalized.count(c) > 1})
        raise ValueError(f"CSV contains duplicate column names after normalization: {duplicates}")
    return df.toDF(*normalized)


def ensure_expected_columns(df: DataFrame) -> DataFrame:
    """Add absent expected fields as nulls and retain unexpected source fields."""
    for column_name in EXPECTED_SOURCE_COLUMNS:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None).cast("string"))

    # Keep the source schema in a stable order, followed by any extra CSV columns.
    extras = [c for c in df.columns if c not in EXPECTED_SOURCE_COLUMNS]
    return df.select(*EXPECTED_SOURCE_COLUMNS, *extras)


def _nullify_blank(column_name: str):
    value = F.trim(F.col(column_name).cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def _write_delta_table(df: DataFrame, table_name: str) -> None:
    """Idempotently replace a derived table for this single-snapshot pipeline."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def _table(catalog: str, schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"


def ingest_bronze(
    spark: SparkSession,
    source_csv_path: str,
    bronze_table: str,
    run_id: str,
) -> DataFrame:
    """Read the supplied CSV as strings and persist an auditable Bronze snapshot."""
    if not source_csv_path or not source_csv_path.strip():
        raise ValueError("source_csv_path must be supplied.")

    source_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .load(source_csv_path)
    )

    source_df = ensure_expected_columns(normalize_headers(source_df))
    # Hash the original CSV values before trimming/casting in Silver so that
    # duplicate detection reflects exact source-record equality.
    bronze_df = (
        source_df.withColumn(
            "record_hash",
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in EXPECTED_SOURCE_COLUMNS])), 256),
        )
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )

    _write_delta_table(bronze_df, bronze_table)
    return spark.table(bronze_table)


def transform_silver(bronze_df: DataFrame) -> DataFrame:
    """Standardize source values, cast types and add auditable quality flags."""
    df = bronze_df

    for column_name in STRING_COLUMNS:
        if column_name in df.columns:
            df = df.withColumn(column_name, _nullify_blank(column_name))

    for column_name in INTEGER_COLUMNS:
        if column_name in df.columns:
            raw_column = f"_{column_name}_raw"
            raw_value = F.trim(F.col(column_name).cast("string"))
            df = (
                df.withColumn(raw_column, F.when(raw_value == "", F.lit(None).cast("string")).otherwise(raw_value))
                .withColumn(column_name, F.expr(f"try_cast(`{raw_column}` AS BIGINT)"))
            )

    for column_name in FLOAT_COLUMNS:
        if column_name in df.columns:
            raw_column = f"_{column_name}_raw"
            raw_value = F.trim(F.col(column_name).cast("string"))
            df = (
                df.withColumn(raw_column, F.when(raw_value == "", F.lit(None).cast("string")).otherwise(raw_value))
                .withColumn(column_name, F.expr(f"try_cast(`{raw_column}` AS DOUBLE)"))
            )

    for column_name in DATE_COLUMNS:
        if column_name in df.columns:
            raw_column = f"_{column_name}_raw"
            raw_value = F.trim(F.col(column_name).cast("string"))
            df = df.withColumn(
                raw_column,
                F.when(raw_value == "", F.lit(None).cast("string")).otherwise(raw_value),
            )
            raw_identifier = f"`{raw_column}`"
            df = df.withColumn(
                column_name,
                F.coalesce(
                    F.expr(f"try_to_timestamp({raw_identifier}, 'yyyy-MM-dd')").cast("date"),
                    F.expr(f"try_to_timestamp({raw_identifier}, 'dd/MM/yyyy')").cast("date"),
                    F.expr(f"try_to_timestamp({raw_identifier}, 'd/M/yyyy')").cast("date"),
                ),
            )

    # Colour fields are standardised to uppercase hexadecimal notation.
    for colour_column in ["text_colour", "background_colour", "foreground_colour"]:
        if colour_column in df.columns:
            df = df.withColumn(colour_column, F.upper(F.col(colour_column)))

    route_label = F.coalesce(
        F.col("route_search_name"),
        F.col("efa_route_name"),
        F.col("my_timetable_route_name"),
    )
    df = df.withColumn("route_label", route_label)

    # Bronze provides a hash of the original CSV values. This fallback supports
    # direct unit tests or callers that construct Silver input independently.
    if "record_hash" not in df.columns:
        source_fields = [F.col(c) for c in EXPECTED_SOURCE_COLUMNS]
        df = df.withColumn(
            "record_hash",
            F.sha2(F.to_json(F.struct(*source_fields)), 256),
        )

    duplicate_window = Window.partitionBy("record_hash")
    duplicate_order = Window.partitionBy("record_hash").orderBy(
        F.col("_source_file").asc_nulls_last(), F.col("_run_id").asc_nulls_last()
    )
    df = (
        df.withColumn("_exact_duplicate_group_size", F.count(F.lit(1)).over(duplicate_window))
        .withColumn("_duplicate_instance_number", F.row_number().over(duplicate_order))
        .withColumn("is_exact_duplicate", F.col("_exact_duplicate_group_size") > 1)
        .withColumn("is_duplicate_extra_row", F.col("_duplicate_instance_number") > 1)
    )

    has_gtfs_route_id = (
        F.col("gtfs_route_id_in").isNotNull()
        | F.col("gtfs_route_id_out").isNotNull()
    )
    has_agency_id = F.col("gtfs_agency_id").isNotNull()
    candidate_identity = F.struct(
        F.col("gtfs_agency_id").alias("agency_id"),
        F.col("gtfs_route_id_in").alias("route_id_in"),
        F.col("gtfs_route_id_out").alias("route_id_out"),
        F.col("route_label").alias("route_label"),
    )
    df = df.withColumn(
        "route_candidate_key",
        F.when(
            has_agency_id & has_gtfs_route_id,
            F.sha2(F.to_json(candidate_identity), 256),
        ).otherwise(F.lit(None).cast("string")),
    )
    df = df.withColumn(
        "route_identity_method",
        F.when(F.col("route_candidate_key").isNotNull(), F.lit("agency + GTFS route IDs + route label"))
        .otherwise(F.lit("unresolved - missing agency or GTFS route ID")),
    )
    return df


def build_data_quality_issues(silver_df: DataFrame) -> DataFrame:
    """Create one row per quality issue without modifying the affected source row."""
    issue_rules = [
        (
            "MISSING_OPERATOR_ID",
            "ERROR",
            "operator_id",
            "Operator identifier is missing.",
            F.col("operator_id").isNull(),
        ),
        (
            "MISSING_OPERATOR_NAME",
            "WARNING",
            "operator_name",
            "Operator name is missing.",
            F.col("operator_name").isNull(),
        ),
        (
            "MISSING_ROUTE_NAME",
            "WARNING",
            "route_label",
            "No route name is available in the supplied route-name fields.",
            F.col("route_label").isNull(),
        ),
        (
            "MISSING_GTFS_AGENCY_ID",
            "WARNING",
            "gtfs_agency_id",
            "GTFS agency identifier is missing.",
            F.col("gtfs_agency_id").isNull(),
        ),
        (
            "MISSING_GTFS_ROUTE_ID",
            "WARNING",
            "gtfs_route_id_in/gtfs_route_id_out",
            "Both inbound and outbound GTFS route identifiers are missing.",
            F.col("gtfs_route_id_in").isNull() & F.col("gtfs_route_id_out").isNull(),
        ),
        (
            "MISSING_CONTRACT_ID",
            "INFO",
            "contract_id",
            "Contract identifier is not populated in the source record.",
            F.col("contract_id").isNull(),
        ),
        (
            "MISSING_OPERATOR_CONTACT",
            "INFO",
            "lp_contact_number/lp_email",
            "Both operator contact number and email are missing.",
            F.col("lp_contact_number").isNull() & F.col("lp_email").isNull(),
        ),
        (
            "MISSING_ROUTE_VARIANT_TYPE",
            "INFO",
            "route_variant_type",
            "Route variant type is missing.",
            F.col("route_variant_type").isNull(),
        ),
        (
            "INVALID_FREQUENCY_ROUTE_CAST",
            "WARNING",
            "frequency_route",
            "frequency_route contains a non-numeric value that could not be cast to an integer.",
            F.col("_frequency_route_raw").isNotNull() & F.col("frequency_route").isNull(),
        ),
        (
            "INVALID_FREQUENCY_ROUTE_FLAG",
            "WARNING",
            "frequency_route",
            "Expected frequency_route to be 0 or 1.",
            F.col("frequency_route").isNotNull() & (~F.col("frequency_route").isin(0, 1)),
        ),
        (
            "INVALID_ROUTE_TYPE_CAST",
            "WARNING",
            "route_type",
            "route_type contains a non-numeric value that could not be cast to an integer.",
            F.col("_route_type_raw").isNotNull() & F.col("route_type").isNull(),
        ),
        (
            "INVALID_TARIFF_TYPE_CAST",
            "WARNING",
            "tariff_type",
            "tariff_type contains a non-numeric value that could not be cast to a number.",
            F.col("_tariff_type_raw").isNotNull() & F.col("tariff_type").isNull(),
        ),
        (
            "INVALID_START_DATE",
            "WARNING",
            "start_date",
            "start_date is populated but could not be parsed using supported date formats.",
            F.col("_start_date_raw").isNotNull() & F.col("start_date").isNull(),
        ),
        (
            "INVALID_COLOUR_CODE",
            "WARNING",
            "text_colour/background_colour/foreground_colour",
            "A colour value is not in #RRGGBB hexadecimal format.",
            (
                F.col("text_colour").isNotNull()
                & (~F.col("text_colour").rlike(r"^#[0-9A-F]{6}$"))
            )
            | (
                F.col("background_colour").isNotNull()
                & (~F.col("background_colour").rlike(r"^#[0-9A-F]{6}$"))
            )
            | (
                F.col("foreground_colour").isNotNull()
                & (~F.col("foreground_colour").rlike(r"^#[0-9A-F]{6}$"))
            ),
        ),
        (
            "EXACT_DUPLICATE_EXTRA_ROW",
            "WARNING",
            "all source fields",
            "This row is an additional occurrence of an exact source-record duplicate.",
            F.col("is_duplicate_extra_row"),
        ),
    ]

    issue_frames: List[DataFrame] = []
    for code, severity, field_name, description, condition in issue_rules:
        issue_frames.append(
            silver_df.filter(condition).select(
                "record_hash",
                "route_candidate_key",
                "operator_id",
                "operator_name",
                "route_label",
                F.lit(code).alias("issue_code"),
                F.lit(severity).alias("severity"),
                F.lit(field_name).alias("field_name"),
                F.lit(description).alias("issue_description"),
                F.col("_source_file").alias("source_file"),
                F.col("_run_id").alias("run_id"),
            )
        )

    return reduce(lambda left, right: left.unionByName(right), issue_frames)


def build_field_completeness(silver_df: DataFrame) -> DataFrame:
    """Return one row per source column with missing and populated counts."""
    source_column_names = [c for c in EXPECTED_SOURCE_COLUMNS if c in silver_df.columns]
    total_rows = silver_df.count()
    if total_rows == 0:
        rows = [(name, 0, 0, 0, 0.0) for name in source_column_names]
    else:
        expressions = [
            F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(f"missing_{index}")
            for index, name in enumerate(source_column_names)
        ]
        metric_row = silver_df.agg(*expressions).first()
        rows = []
        for index, name in enumerate(source_column_names):
            missing = int(metric_row[f"missing_{index}"] or 0)
            populated = total_rows - missing
            rows.append((name, total_rows, missing, populated, populated / total_rows))

    schema = """field_name STRING, total_rows LONG, missing_count LONG,
                  populated_count LONG, completeness_pct DOUBLE"""
    return silver_df.sparkSession.createDataFrame(rows, schema=schema)


def build_gold_tables(silver_df: DataFrame) -> Dict[str, DataFrame]:
    """Build Power BI-ready aggregates from the cleaned, fully retained Silver data."""
    operator_summary = (
        silver_df.groupBy("operator_id", "operator_name")
        .agg(
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("route_candidate_key").alias("route_candidate_count"),
            F.countDistinct("route_label").alias("distinct_route_name_count"),
            F.countDistinct("contract_id").alias("distinct_contract_count"),
            F.sum(F.when(F.col("lp_email").isNotNull(), 1).otherwise(0)).alias("records_with_email"),
            F.sum(F.when(F.col("lp_contact_number").isNotNull(), 1).otherwise(0)).alias("records_with_phone"),
            F.sum(F.when(F.col("is_duplicate_extra_row"), 1).otherwise(0)).alias("duplicate_extra_rows"),
        )
        .withColumn(
            "email_record_coverage",
            F.when(F.col("source_record_count") > 0, F.col("records_with_email") / F.col("source_record_count")),
        )
        .withColumn(
            "phone_record_coverage",
            F.when(F.col("source_record_count") > 0, F.col("records_with_phone") / F.col("source_record_count")),
        )
    )

    transport_summary = (
        silver_df.groupBy("transport_name", "route_variant_type", "route_type")
        .agg(
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("route_candidate_key").alias("route_candidate_count"),
            F.countDistinct("route_label").alias("distinct_route_name_count"),
            F.countDistinct("operator_id").alias("operator_count"),
            F.sum(F.when(F.col("is_duplicate_extra_row"), 1).otherwise(0)).alias("duplicate_extra_rows"),
        )
    )

    route_register = (
        silver_df.filter(F.col("route_candidate_key").isNotNull())
        .groupBy("route_candidate_key")
        .agg(
            F.first("gtfs_agency_id", ignorenulls=True).alias("gtfs_agency_id"),
            F.first("gtfs_route_id_in", ignorenulls=True).alias("gtfs_route_id_in"),
            F.first("gtfs_route_id_out", ignorenulls=True).alias("gtfs_route_id_out"),
            F.first("route_label", ignorenulls=True).alias("route_label"),
            F.first("route_search_name", ignorenulls=True).alias("route_search_name"),
            F.first("efa_route_name", ignorenulls=True).alias("efa_route_name"),
            F.sort_array(F.collect_set("operator_name")).alias("operator_names"),
            F.sort_array(F.collect_set("operator_id")).alias("operator_ids"),
            F.sort_array(F.collect_set("transport_name")).alias("transport_names"),
            F.sort_array(F.collect_set("route_variant_type")).alias("route_variant_types"),
            F.count(F.lit(1)).alias("source_record_count"),
            F.countDistinct("operator_id").alias("operator_count"),
            F.countDistinct("route_variant_type").alias("route_variant_count"),
            F.sum(F.when(F.col("is_duplicate_extra_row"), 1).otherwise(0)).alias("duplicate_extra_rows"),
        )
        .withColumn("route_identity_confidence", F.lit("Candidate - validate against GTFS routes.txt"))
    )

    network_overview = silver_df.agg(
        F.count(F.lit(1)).alias("source_record_count"),
        F.countDistinct("route_candidate_key").alias("route_candidate_count"),
        F.countDistinct("route_label").alias("distinct_route_name_count"),
        F.countDistinct("operator_id").alias("distinct_operator_id_count"),
        F.countDistinct("operator_name").alias("distinct_operator_name_count"),
        F.sum(F.when(F.col("route_variant_type") == "School", 1).otherwise(0)).alias("school_variant_record_count"),
        F.sum(F.when(F.col("is_duplicate_extra_row"), 1).otherwise(0)).alias("exact_duplicate_extra_row_count"),
        F.sum(F.when(F.col("contract_id").isNull(), 1).otherwise(0)).alias("missing_contract_id_count"),
        F.sum(F.when(F.col("route_variant_type").isNull(), 1).otherwise(0)).alias("missing_route_variant_type_count"),
        F.sum(F.when(F.col("route_candidate_key").isNull(), 1).otherwise(0)).alias("unresolved_route_identity_record_count"),
    ).withColumn(
        "school_variant_record_share",
        F.when(
            F.col("source_record_count") > 0,
            F.col("school_variant_record_count") / F.col("source_record_count"),
        ),
    )

    return {
        "operator_summary": operator_summary,
        "transport_summary": transport_summary,
        "route_register": route_register,
        "network_overview": network_overview,
        "field_completeness": build_field_completeness(silver_df),
    }


def run_pipeline(
    spark: SparkSession,
    source_csv_path: str,
    catalog: str = DEFAULT_CATALOG,
    bronze_schema: str = DEFAULT_BRONZE_SCHEMA,
    silver_schema: str = DEFAULT_SILVER_SCHEMA,
    gold_schema: str = DEFAULT_GOLD_SCHEMA,
    run_id: Optional[str] = None,
) -> Dict[str, object]:
    """Run the first CSV snapshot pipeline and return table names and row counts."""
    catalog = validate_identifier(catalog, "catalog")
    bronze_schema = validate_identifier(bronze_schema, "bronze schema")
    silver_schema = validate_identifier(silver_schema, "silver schema")
    gold_schema = validate_identifier(gold_schema, "gold schema")
    run_id = run_id or str(uuid.uuid4())

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

    bronze_table = _table(catalog, bronze_schema, "bus_route_metadata_raw")
    silver_table = _table(catalog, silver_schema, "bus_route_metadata")

    bronze_df = ingest_bronze(spark, source_csv_path, bronze_table, run_id)
    silver_df = transform_silver(bronze_df)
    _write_delta_table(silver_df, silver_table)

    issues_df = build_data_quality_issues(silver_df)
    issue_table = _table(catalog, gold_schema, "data_quality_issues")
    _write_delta_table(issues_df, issue_table)

    gold_frames = build_gold_tables(silver_df)
    table_names: Dict[str, str] = {
        "bronze": bronze_table,
        "silver": silver_table,
        "data_quality_issues": issue_table,
    }
    for name, frame in gold_frames.items():
        full_name = _table(catalog, gold_schema, name)
        _write_delta_table(frame, full_name)
        table_names[name] = full_name

    quality_summary = (
        spark.table(issue_table)
        .groupBy("issue_code", "severity", "field_name")
        .agg(
            F.count(F.lit(1)).alias("issue_count"),
            F.countDistinct("record_hash").alias("affected_record_count"),
        )
        .orderBy(F.desc("issue_count"), F.asc("issue_code"))
    )
    quality_summary_table = _table(catalog, gold_schema, "data_quality_summary")
    _write_delta_table(quality_summary, quality_summary_table)
    table_names["data_quality_summary"] = quality_summary_table

    counts = {
        "bronze_rows": spark.table(bronze_table).count(),
        "silver_rows": spark.table(silver_table).count(),
        "route_candidates": spark.table(table_names["route_register"]).count(),
        "data_quality_issues": spark.table(issue_table).count(),
    }

    return {
        "run_id": run_id,
        "tables": table_names,
        "counts": counts,
    }
