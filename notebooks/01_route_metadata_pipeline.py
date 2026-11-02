# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Pipeline overview
# MAGIC %md
# MAGIC # 01 – NSW Bus Route Metadata: Bronze → Silver → Gold
# MAGIC
# MAGIC This notebook runs the initial CSV pipeline. It preserves source rows,
# MAGIC flags exact duplicates and quality issues, and creates Power BI-ready Gold tables.
# MAGIC
# MAGIC **Outputs** are written to the `nsw_bus_data` catalog in the `bronze`, `silver` and `gold` schemas.
# MAGIC
# MAGIC **Before running:** upload the TfNSW route metadata CSV to
# MAGIC `/Volumes/nsw_bus_data/default/landing/bus_routes.csv`.

# COMMAND ----------

# DBTITLE 1,Pipeline configuration widgets
# Databricks widgets: change these values for your workspace.
dbutils.widgets.text(
    "source_csv_path",
    "/Volumes/nsw_bus_data/default/landing/bus_routes.csv",
    "Source CSV path",
)
dbutils.widgets.text("catalog", "nsw_bus_data", "Unity Catalog catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")

# COMMAND ----------

# DBTITLE 1,Import project pipeline package
import sys
from pathlib import Path


def find_repo_root(marker="src/pipeline.py") -> str:
    """Locate the project root containing the src package.

    The notebook working directory is not always the repository root, so we
    search the current directory, its parents, and the workspace home folder.
    """
    cwd = Path.cwd()
    home = Path("/Workspace/Users/moeedk1@gmail.com")
    candidates = [cwd, *cwd.parents, home, *home.glob("*/")]
    for candidate in candidates:
        if (candidate / marker).is_file():
            return str(candidate)
    raise FileNotFoundError(
        "Could not locate the project root containing src/pipeline.py. "
        "Verify the NSW-Bus-Route project folder exists in your workspace."
    )


repo_root = find_repo_root()
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.pipeline import run_pipeline  # noqa: E402

print("Project root:", repo_root)

# COMMAND ----------

# DBTITLE 1,Pre-flight checks
# Pre-flight checks: validate identifiers and confirm the source CSV exists before
# running the pipeline, so failures surface with actionable messages.
from src.pipeline import validate_identifier

source_csv_path = dbutils.widgets.get("source_csv_path").strip()
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()

for label, value in [
    ("catalog", catalog),
    ("bronze schema", bronze_schema),
    ("silver schema", silver_schema),
    ("gold schema", gold_schema),
]:
    validate_identifier(value, label)

try:
    dbutils.fs.ls(source_csv_path)
except Exception:
    raise FileNotFoundError(
        f"Source CSV not found at '{source_csv_path}'. Upload the TfNSW route "
        "metadata CSV to that Unity Catalog volume path, then re-run this notebook."
    )

# The pipeline creates the bronze/silver/gold schemas if they do not exist yet.
print(f"Source file found: {source_csv_path}")
print(f"Tables will be written to {catalog}.{bronze_schema}, {catalog}.{silver_schema} and {catalog}.{gold_schema}")

# COMMAND ----------

result = run_pipeline(
    spark=spark,
    source_csv_path=dbutils.widgets.get("source_csv_path").strip(),
    catalog=dbutils.widgets.get("catalog").strip(),
    bronze_schema=dbutils.widgets.get("bronze_schema").strip(),
    silver_schema=dbutils.widgets.get("silver_schema").strip(),
    gold_schema=dbutils.widgets.get("gold_schema").strip(),
)

print("Pipeline completed")
print("Run ID:", result["run_id"])
print("Counts:", result["counts"])
print("Tables:")
for layer, table_name in result["tables"].items():
    print(f"  {layer}: {table_name}")

# COMMAND ----------

# Quick inspection of the main outputs.
display(spark.table(result["tables"]["network_overview"]))

# COMMAND ----------

display(spark.table(result["tables"]["operator_summary"]).orderBy("source_record_count", ascending=False).limit(20))

# COMMAND ----------

display(spark.table(result["tables"]["transport_summary"]).orderBy("source_record_count", ascending=False))

# COMMAND ----------

display(spark.table(result["tables"]["data_quality_summary"]).orderBy("issue_count", ascending=False))