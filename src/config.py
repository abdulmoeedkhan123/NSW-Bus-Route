"""Project configuration and source schema for the TfNSW route metadata CSV."""

EXPECTED_SOURCE_COLUMNS = [
    "operator_name",
    "operator_id",
    "contract_id",
    "my_timetable_route_name",
    "route_variant_type",
    "transport_name_id",
    "transport_name",
    "mot_for_interchange_id",
    "mot_for_interchange",
    "efa_route_name",
    "route_search_name",
    "service_direction_name",
    "1st_twitter_handle",
    "2nd_twitter_handle",
    "tariff_type",
    "depot_name",
    "lp_contact_number",
    "lp_weekday_hours",
    "lp_email",
    "region",
    "regional_trains",
    "text_colour",
    "background_colour",
    "foreground_colour",
    "start_date",
    "route_type",
    "frequency_route",
    "gtfs_agency_id",
    "gtfs_route_id_in",
    "gtfs_route_id_out",
    "gtfs_alerts_id",
    "gtfs_alerts_name",
]

STRING_COLUMNS = [
    "operator_name",
    "operator_id",
    "contract_id",
    "my_timetable_route_name",
    "route_variant_type",
    "transport_name",
    "mot_for_interchange",
    "efa_route_name",
    "route_search_name",
    "service_direction_name",
    "1st_twitter_handle",
    "2nd_twitter_handle",
    "depot_name",
    "lp_contact_number",
    "lp_weekday_hours",
    "lp_email",
    "region",
    "regional_trains",
    "text_colour",
    "background_colour",
    "foreground_colour",
    "gtfs_agency_id",
    "gtfs_route_id_in",
    "gtfs_route_id_out",
    "gtfs_alerts_name",
]

INTEGER_COLUMNS = [
    "transport_name_id",
    "mot_for_interchange_id",
    "route_type",
    "frequency_route",
    "gtfs_alerts_id",
]

FLOAT_COLUMNS = ["tariff_type"]
DATE_COLUMNS = ["start_date"]

# Columns that should normally identify a populated route record.
# Route identity is still treated as a candidate, not a canonical route key.
CORE_ROUTE_COLUMNS = [
    "gtfs_agency_id",
    "gtfs_route_id_in",
    "gtfs_route_id_out",
]

DEFAULT_CATALOG = "main"
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"
