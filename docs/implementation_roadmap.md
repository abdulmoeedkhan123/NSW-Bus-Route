# Implementation roadmap

## Milestone 1 – Route metadata Lakehouse (implemented in code)

- Ingest the 32-column CSV into Bronze.
- Preserve original values and attach source file, run ID and ingestion timestamp.
- Standardise blanks, strings, colour codes, numeric fields and dates in Silver.
- Detect exact duplicate source records and retain them with flags.
- Generate a provisional GTFS-backed route candidate key.
- Create Gold operator, transport, route-register, network-overview and quality outputs.
- Validate output counts and review data-quality findings.

## Milestone 2 – Complete GTFS static integration

Ingest the official GTFS ZIP as raw files, preserving the original feed version and extraction date. Parse at least:

- `agency.txt`
- `routes.txt`
- `trips.txt`
- `stops.txt`
- `stop_times.txt`
- `calendar.txt`
- `calendar_dates.txt`
- `shapes.txt`
- `frequencies.txt` when supplied

Validate foreign keys: trips to routes and service calendars; stop times to trips and stops; shapes to trips; calendar exceptions to service IDs. Join metadata candidates to the official agency and route records; document unmatched and one-to-many matches rather than forcing a join.

Potential Gold additions:

- `dim_agency`, `dim_route`, `dim_stop`, `dim_service_date`, `dim_time`
- `fact_trip`, `fact_stop_time`, `fact_route_service`
- route geometry and stop coverage aggregates

Possible analyses: scheduled trips by route/day/time, first and last scheduled service, headway estimates from stop times, stop density and route coverage. These describe scheduled service, not actual punctuality.

## Milestone 3 – Power BI

Build report pages for:

1. Network overview – record counts, candidate route counts, operators, categories and data quality.
2. Operator intelligence – source records, candidate routes, contract and contact completeness.
3. Route and service intelligence – route maps, stops, trips, operating calendars and scheduled headways after GTFS integration.
4. Data quality – missing values, duplicate rows, unmatched agency/route IDs and referential-integrity failures.

## Milestone 4 – GTFS-Realtime and operational monitoring

After static GTFS IDs are reconciled, ingest the real-time vehicle positions, trip updates and service alerts. Store raw feed snapshots with ingestion timestamps and feed timestamps. Decode Protocol Buffers using the feed's `.proto` definitions. Build historical observations before calculating delay or reliability metrics.

Potential analysis: observed vehicle positions, alert volumes, schedule-versus-observed arrival differences and route-level delay distributions. Capture feed freshness and missing-feed intervals so that gaps are not mistaken for service absence.

## Milestone 5 – ML and AI assistant

Only after adequate history exists:

- Anomaly detection on feed/operational metrics.
- Scheduled-service pattern clustering.
- Delay prediction if historical schedule-versus-actual observations are available and representative.
- Read-only natural-language analytics assistant over curated Gold tables, with SQL validation, row limits, query logging and no write permissions.
