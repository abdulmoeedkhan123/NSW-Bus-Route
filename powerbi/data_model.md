# Power BI starter model

## Recommended first-load tables

Load these Gold tables from the Databricks SQL Warehouse:

- `network_overview` – a single-row KPI table.
- `operator_summary` – one row per `(operator_id, operator_name)` combination.
- `transport_summary` – one row per transport name, route variant and route type combination.
- `route_register` – one row per provisional `route_candidate_key`.
- `data_quality_summary` – grouped quality-rule counts.
- `field_completeness` – one row per source field.

Optionally load `data_quality_issues` for a drill-through detail page. It may contain operator contact-related issue rows, so apply the appropriate access controls and avoid exposing contact information to broad report audiences.

## Relationships

For the initial release, keep `network_overview`, `data_quality_summary` and `field_completeness` disconnected KPI/diagnostic tables. The `operator_summary` and `route_register` tables can be linked only after confirming the operator identifier relationship and deciding whether route candidates can legitimately map to more than one operator. Do not create a many-to-many relationship merely to make visuals filter.

When GTFS is integrated, build conformed dimensions for agency, route, stop and service date, and use explicit one-to-many relationships to facts such as trips, stop times and vehicle observations.

## Suggested report pages

1. **Network Overview** – cards for source rows, route candidates, distinct route labels, operator IDs, school variant share and unresolved route identities.
2. **Operator Intelligence** – ranked operator source records, route candidates, contact completeness and contract counts.
3. **Transport Categories** – records by transport name, variant type and route type.
4. **Data Quality** – missingness by field, issue counts by severity and issue code, duplicate extra rows.

## Interpretation notes

- “Source records” means rows in the supplied CSV.
- “Candidate route identities” are generated using a provisional key and must be reconciled to official GTFS `routes.txt`.
- “Distinct route names” is a text-label count, not proof of unique physical or scheduled routes.
- “School variant record share” is based on rows whose `route_variant_type` equals the exact text `School`; it is not a share of trips or passenger journeys.
- Completeness percentages are calculated after Silver standardisation and type conversion.
