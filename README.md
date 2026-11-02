# NSW Bus Network Intelligence

An end-to-end portfolio project using Transport for NSW bus-route metadata, Databricks, PySpark, Delta Lake and Power BI. The first milestone turns the supplied 13,610-row / 32-column CSV into auditable Bronze, cleaned Silver and analytics-ready Gold tables. Later milestones can integrate complete GTFS timetables and GTFS-Realtime feeds.

## Current scope

**Implemented in this repository**

- CSV ingestion with source-file, ingestion-time and run-ID metadata.
- Bronze Delta snapshot table; source fields remain strings and are not overwritten.
- Silver transformations: whitespace cleanup, blank-to-null standardisation, typed numeric/date fields, colour normalisation and route-name fallback.
- Exact source-row duplicate detection using a hash created before Silver transformations. Duplicates are flagged, not deleted.
- A provisional `route_candidate_key` based on GTFS agency ID, inbound/outbound route IDs and route label. It is deliberately labelled a candidate until reconciled to the official GTFS `routes.txt` feed.
- Gold tables for operator summaries, transport categories, candidate route register, network overview, field completeness and data-quality issues.
- A Databricks notebook, SQL inspection queries, starter DAX measures and basic Spark tests.

**Not implemented yet**

- Complete GTFS joins for `agency`, `routes`, `trips`, `stops`, `stop_times`, `calendar` and `shapes`.
- Geospatial route coverage and stop catchment analysis.
- Real-time vehicle positions, trip updates or service alerts.
- Passenger demand, punctuality, delay or travel-time analytics. Those require additional source data and should not be inferred from this route metadata CSV.
- AI copilot or predictive modelling.

## Repository layout

```text
NSW-Bus-Network-Intelligence/
├── src/
│   ├── __init__.py
│   ├── config.py
│   └── pipeline.py
├── notebooks/
│   └── 01_route_metadata_pipeline.py
├── sql/
│   └── 01_inspect_gold_tables.sql
├── powerbi/
│   ├── dax_measures.dax
│   └── data_model.md
├── tests/
│   └── test_pipeline.py
├── docs/
│   ├── data_dictionary.md
│   └── implementation_roadmap.md
├── data/sample/
│   └── README.md
├── requirements-dev.txt
└── README.md
```

## 1. Prepare Databricks

1. Create or select a Unity Catalog catalog that you can write to. The notebook defaults to `main`; change it if your workspace uses another catalog.
2. Create a Unity Catalog Volume named `landing` in a schema you can access. For the default settings, the target path is:

   `/Volumes/main/default/landing/`

3. Upload the original route CSV into that Volume, for example:

   `/Volumes/main/default/landing/bus_routes.csv`

   Keep the original file unchanged. The project reads it and creates Delta tables separately.
4. Add this project to a Databricks Git folder (recommended), or upload the project files into your workspace.
5. Open `notebooks/01_route_metadata_pipeline.py` and attach a Databricks Runtime with Spark and Delta support. No extra Python packages are required for the pipeline in Databricks.

If you do not have Unity Catalog Volumes available, use a storage path approved by your Databricks administrator and paste that path into the notebook widget. Avoid hardcoding personal credentials or storage keys in notebooks.

## 2. Run the first pipeline

Open the notebook and set these widgets:

| Widget | Default | Meaning |
|---|---|---|
| `source_csv_path` | `/Volumes/main/default/landing/bus_routes.csv` | Actual CSV location |
| `catalog` | `main` | Existing Unity Catalog catalog |
| `bronze_schema` | `bronze` | Raw ingestion schema |
| `silver_schema` | `silver` | Cleaned and typed schema |
| `gold_schema` | `gold` | Analytics schema |

Run all cells. The pipeline creates schemas if they do not already exist and overwrites the derived snapshot tables on each run. For this first single-file milestone, overwrite makes reruns reproducible. Before introducing periodic source snapshots, we will add a historical ingestion design rather than simply overwrite Bronze.

### Tables created

| Layer | Table | Purpose |
|---|---|---|
| Bronze | `main.bronze.bus_route_metadata_raw` | Original source values plus ingestion metadata and source hash |
| Silver | `main.silver.bus_route_metadata` | Typed, cleaned rows; duplicates retained and flagged |
| Gold | `main.gold.operator_summary` | Operator-level record and route-candidate summaries |
| Gold | `main.gold.transport_summary` | Records by transport, route variant and route type |
| Gold | `main.gold.route_register` | Provisional GTFS-backed route candidates |
| Gold | `main.gold.network_overview` | Single-row KPI table |
| Gold | `main.gold.data_quality_issues` | One row per detected issue |
| Gold | `main.gold.data_quality_summary` | Issue counts grouped by rule and severity |
| Gold | `main.gold.field_completeness` | Missing/populated counts and completeness percentage for each source field |

The notebook prints counts for Bronze, Silver, route candidates and issue rows. Check that Bronze and Silver both contain the same number of source records. The project should not silently remove source records.

## 3. First validation checklist

After the first run, validate these items:

- Bronze row count equals the CSV row count (your supplied profile reported 13,610 rows).
- Silver row count equals Bronze row count.
- The source CSV has the expected 32 columns. Missing expected columns are added as nulls; unexpected columns are retained.
- Compare `exact_duplicate_extra_row_count` with an independent check in pandas or Spark.
- Inspect `route_candidate_count` and `unresolved_route_identity_record_count`; treat the candidate count as provisional.
- Review `field_completeness` and data-quality issues before deciding whether a field is truly mandatory.
- Compare operator counts by `operator_id` and `operator_name`; your supplied profile shows these have different distinct counts.

Your pasted profile reports 466 distinct operator names and 691 distinct operator IDs. Do not label either figure simply “the number of operators” until the agency/operator identifier relationship is understood.

## 4. Connect Power BI

Use the Databricks SQL Warehouse connector in Power BI and connect to the Gold schema. Suggested first tables:

- `network_overview`
- `operator_summary`
- `transport_summary`
- `route_register`
- `data_quality_summary`

Start with Import mode for the small initial metadata dataset. Use DirectQuery only if the deployment, refresh and governance requirements call for it. See `powerbi/data_model.md` and `powerbi/dax_measures.dax`.

## 5. Run tests locally (optional)

Databricks already provides PySpark. For local development, install Java compatible with your PySpark version, then:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

The tests use a local Spark session and cover type conversion, candidate route identity, duplicate flagging, Gold aggregates and missing GTFS route IDs. Local Spark tests were not executed while this project was generated because PySpark is not installed in the generation environment.

## 6. Data handling and interpretation rules

- Treat the supplied CSV as route metadata, not a record of journeys or actual vehicle operations.
- `source_record_count` is a count of source rows, not a count of scheduled trips or unique real-world services.
- `distinct_route_name_count` is a count of route labels, not a verified unique route count.
- `route_candidate_key` is a provisional matching key. Reconcile it with the official GTFS `routes.txt` before describing it as a canonical route identifier.
- Do not infer service frequency from `frequency_route` alone. The supplied profile shows it is a 0/1 field; timetable frequency needs `trips.txt`, `stop_times.txt` and potentially `frequencies.txt`.
- Do not infer passenger volumes, delays, reliability or coverage without appropriate datasets.
- Contact and Twitter fields may be incomplete or outdated; do not publish personal information or operational contacts without checking permitted use.

## 7. Next implementation milestones

See `docs/implementation_roadmap.md`. The next technical increment is GTFS static integration: parse `agency.txt`, `routes.txt`, `trips.txt`, `stops.txt`, `stop_times.txt`, `calendar.txt`, `calendar_dates.txt` and `shapes.txt`; validate referential integrity; then add route geometry, service calendars, trip counts and scheduled service frequency.
