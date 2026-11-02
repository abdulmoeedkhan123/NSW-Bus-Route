"""Small Spark tests for the initial route metadata transformations.

Run inside Databricks or locally with PySpark and pytest installed:
    pytest -q
"""

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

from src.config import EXPECTED_SOURCE_COLUMNS
from src.pipeline import build_data_quality_issues, build_gold_tables, transform_silver


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("nsw-bus-intelligence-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(**overrides):
    values = {name: None for name in EXPECTED_SOURCE_COLUMNS}
    values.update(
        {
            "operator_name": "Example Operator",
            "operator_id": "2501",
            "my_timetable_route_name": "S257",
            "route_variant_type": "School",
            "transport_name_id": "9",
            "transport_name": "School buses",
            "mot_for_interchange_id": "11",
            "mot_for_interchange": "Other",
            "efa_route_name": "S257",
            "route_search_name": "S257",
            "service_direction_name": "Inbound",
            "text_colour": "#00B5EF",
            "background_colour": "#FFFFFF",
            "foreground_colour": "#00B5EF",
            "route_type": "712",
            "frequency_route": "0",
            "gtfs_agency_id": "2501",
            "gtfs_route_id_in": "2501_S257",
            "gtfs_route_id_out": "2501_S257",
            "gtfs_alerts_id": "1",
        }
    )
    values.update(overrides)
    return values


def _silver_input(spark, rows):
    schema = StructType([StructField(name, StringType(), True) for name in EXPECTED_SOURCE_COLUMNS])
    frame = spark.createDataFrame(rows, schema=schema)
    return (
        frame.withColumn("_source_file", F.lit("file:/tmp/bus_routes.csv"))
        .withColumn("_run_id", F.lit("test-run"))
    )


def test_transform_silver_types_and_route_candidate(spark):
    raw = _silver_input(spark, [_row()])
    silver = transform_silver(raw)
    result = silver.first()

    assert result["route_type"] == 712
    assert result["frequency_route"] == 0
    assert result["route_candidate_key"] is not None
    assert result["route_label"] == "S257"


def test_duplicate_rows_are_flagged_not_removed(spark):
    row = _row()
    raw = _silver_input(spark, [row, row])
    silver = transform_silver(raw)

    assert silver.count() == 2
    assert silver.filter(F.col("is_exact_duplicate")).count() == 2
    assert silver.filter(F.col("is_duplicate_extra_row")).count() == 1


def test_gold_tables_have_expected_outputs(spark):
    raw = _silver_input(spark, [_row(), _row(my_timetable_route_name="S258", route_search_name="S258", efa_route_name="S258", gtfs_route_id_in="2501_S258")])
    silver = transform_silver(raw)
    gold = build_gold_tables(silver)

    assert gold["network_overview"].first()["source_record_count"] == 2
    assert gold["operator_summary"].count() == 1
    assert gold["route_register"].count() == 2


def test_missing_route_identifier_is_reported(spark):
    raw = _silver_input(spark, [_row(gtfs_route_id_in=None, gtfs_route_id_out=None)])
    silver = transform_silver(raw)
    issues = build_data_quality_issues(silver)

    assert issues.filter(F.col("issue_code") == "MISSING_GTFS_ROUTE_ID").count() == 1
