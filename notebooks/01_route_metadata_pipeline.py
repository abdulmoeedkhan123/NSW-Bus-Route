# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – NSW Bus Route Metadata: Bronze → Silver → Gold
# MAGIC
# MAGIC This notebook runs the initial CSV pipeline. It preserves source rows,
# MAGIC flags exact duplicates and quality issues, and creates Power BI-ready Gold tables.
# MAGIC
# MAGIC **Before running:** upload the TfNSW route metadata CSV to a Unity Catalog Volume
# MAGIC and update the `source_csv_path` widget below.

# COMMAND ----------

# Databricks widgets: change these values for your workspace.
dbutils.widgets.text(
    "source_csv_path",
    "/Volumes/main/default/landing/bus_routes.csv",
    "Source CSV path",
)
dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")

# COMMAND ----------

import os
import sys

# In a Databricks Git folder, the repository root is normally the working directory.
# Add it explicitly so the notebook can import the project package.
repo_root = os.getcwd()
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.pipeline import run_pipeline

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
