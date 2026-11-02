# Source data dictionary – supplied NSW bus route metadata

The names below follow the 32 source columns supplied in the user's DataFrame profile. Types listed as “source” refer to the pandas dtypes shown in that profile. The pipeline preserves raw values in Bronze and casts selected fields in Silver.

| Source column | Source type | Intended meaning / pipeline treatment |
|---|---|---|
| `operator_name` | object | Operator display name; trimmed string |
| `operator_id` | object | Operator identifier; retained as string to preserve leading zeros and formatting |
| `contract_id` | object | Contract identifier; trimmed string |
| `my_timetable_route_name` | object | Timetable route label |
| `route_variant_type` | object | Route variant category, e.g. School or Temporary |
| `transport_name_id` | int64 | Transport category identifier; Silver long |
| `transport_name` | object | Transport category name |
| `mot_for_interchange_id` | int64 | Mode-of-transport interchange identifier; Silver long |
| `mot_for_interchange` | object | Mode-of-transport interchange label |
| `efa_route_name` | object | EFA route label |
| `route_search_name` | object | Route search/display label |
| `service_direction_name` | object | Direction label |
| `1st_twitter_handle` | object | First Twitter/X handle field; retained as string |
| `2nd_twitter_handle` | object | Second Twitter/X handle field; retained as string |
| `tariff_type` | float64 | Tariff type code/value; Silver double |
| `depot_name` | object | Depot name |
| `lp_contact_number` | object | Contact number; kept as string |
| `lp_weekday_hours` | object | Contact weekday hours |
| `lp_email` | object | Contact email |
| `region` | object | Region label |
| `regional_trains` | object | Regional train-related source field; meaning should be confirmed with source documentation |
| `text_colour` | object | Text colour, normalised uppercase |
| `background_colour` | object | Background colour, normalised uppercase |
| `foreground_colour` | object | Foreground colour, normalised uppercase |
| `start_date` | object | Date text; Silver attempts `yyyy-MM-dd`, `dd/MM/yyyy` and `d/M/yyyy` parsing |
| `route_type` | int64 | Route type code; Silver long |
| `frequency_route` | int64 | 0/1 source flag; meaning should be confirmed against TfNSW data documentation |
| `gtfs_agency_id` | object | GTFS agency identifier; retained as string |
| `gtfs_route_id_in` | object | Inbound GTFS route identifier |
| `gtfs_route_id_out` | object | Outbound GTFS route identifier |
| `gtfs_alerts_id` | int64 | Alerts identifier; Silver long |
| `gtfs_alerts_name` | object | Alerts grouping/name |

## Profile figures supplied with the dataset

- 13,610 rows and 32 columns.
- 466 distinct `operator_name` values and 691 distinct `operator_id` values.
- 5,242 distinct `my_timetable_route_name` and 5,242 distinct `efa_route_name` values.
- 11,136 non-null `route_variant_type` values are `School` (the pandas `top`/`freq` output supplied indicates this is the most common value; verify exact distribution using the CSV).
- `contract_id`: 13,132 non-null values.
- `route_variant_type`: 13,192 non-null values.
- `start_date`: 6 non-null values.
- `1st_twitter_handle`: 7,323 non-null values.
- `2nd_twitter_handle`: 30 non-null values.
- `region`: 308 non-null values.
- `regional_trains`: 27 non-null values.

The initial profile summary uses the pandas `describe(include="all")` output pasted in the project conversation. It is a planning input, not a substitute for re-profiling the actual CSV during execution.
