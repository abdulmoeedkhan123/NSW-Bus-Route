-- Run after notebooks/01_route_metadata_pipeline.py.
-- Replace `main` if the pipeline was run in another catalog.

-- High-level KPIs. Note that route_candidate_count is provisional.
SELECT *
FROM main.gold.network_overview;

-- Largest operator portfolios by source records and candidate route identities.
SELECT
    operator_id,
    operator_name,
    source_record_count,
    route_candidate_count,
    distinct_route_name_count,
    email_record_coverage,
    phone_record_coverage,
    duplicate_extra_rows
FROM main.gold.operator_summary
ORDER BY source_record_count DESC
LIMIT 25;

-- Source records by transport and route variant category.
SELECT *
FROM main.gold.transport_summary
ORDER BY source_record_count DESC;

-- Fields with the lowest completeness.
SELECT *
FROM main.gold.field_completeness
ORDER BY completeness_pct ASC, missing_count DESC;

-- Data-quality issues by rule.
SELECT *
FROM main.gold.data_quality_summary
ORDER BY issue_count DESC, issue_code;

-- Inspect a sample of duplicate and incomplete records in Silver.
SELECT
    operator_name,
    operator_id,
    route_label,
    gtfs_agency_id,
    gtfs_route_id_in,
    gtfs_route_id_out,
    is_exact_duplicate,
    is_duplicate_extra_row,
    record_hash,
    _source_file
FROM main.silver.bus_route_metadata
WHERE is_exact_duplicate
   OR route_candidate_key IS NULL
LIMIT 100;
