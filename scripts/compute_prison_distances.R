args <- commandArgs(trailingOnly = TRUE)

mapping_file <- if (length(args) >= 1) args[[1]] else file.path("output", "epc_ppd_mapping.csv")
geocode_file <- if (length(args) >= 2) args[[2]] else file.path("output", "postcode_geocodes.csv")
output_file <- if (length(args) >= 3) args[[3]] else file.path("input", "property_prison_distance.csv")
postcode_filter_file <- if (length(args) >= 4) args[[4]] else file.path("output", "postcode_prison_distance_filter.csv")
postcode_distance_output_file <- if (length(args) >= 5) args[[5]] else file.path("output", "postcode_prison_distances.csv")

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("sf", quietly = TRUE)) {
  stop("Packages 'data.table' and 'sf' are required.")
}

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
}

if (!file.exists(geocode_file)) {
  stop("Geocode file not found: ", geocode_file)
}

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(postcode_distance_output_file), recursive = TRUE, showWarnings = FALSE)

mapping <- data.table::fread(mapping_file)
postcode_points <- data.table::fread(geocode_file)

required_geocode_columns <- c(
  "postcode",
  "geocode_query",
  "latitude",
  "longitude",
  "display_name",
  "geocode_status"
)
missing_geocode_columns <- setdiff(required_geocode_columns, names(postcode_points))
if (length(missing_geocode_columns)) {
  stop(
    "Geocode file is missing required column(s): ",
    paste(missing_geocode_columns, collapse = ", ")
  )
}

postcode_points <- unique(postcode_points, by = "postcode")
postcode_points[, distance_to_prison_m := NA_real_]

prison_location <- data.table::data.table(
  latitude = 52.584126,
  longitude = -1.145212,
  display_name = "HMP Fosse Way (fixed point)",
  geocode_status = "fixed_point"
)

valid_points <- postcode_points[
  !is.na(latitude) & !is.na(longitude) & geocode_status == "ok"
]

if (nrow(valid_points)) {
  properties_sf <- sf::st_as_sf(
    valid_points,
    coords = c("longitude", "latitude"),
    crs = 4326,
    remove = FALSE
  )
  properties_sf <- sf::st_transform(properties_sf, 27700)

  prison_sf <- sf::st_sfc(
    sf::st_point(c(prison_location$longitude, prison_location$latitude)),
    crs = 4326
  )
  prison_sf <- sf::st_transform(prison_sf, 27700)

  valid_points[, distance_to_prison_m := as.numeric(sf::st_distance(properties_sf, prison_sf))]

  postcode_points[
    valid_points,
    on = "postcode",
    distance_to_prison_m := i.distance_to_prison_m
  ]
}

distance_lookup <- merge(
  unique(mapping[, .(ppd_id, lmk_key, match_key, postcode)]),
  postcode_points[, .(
    postcode,
    geocode_query,
    latitude,
    longitude,
    display_name,
    geocode_status,
    distance_to_prison_m
  )],
  by = "postcode",
  all.x = TRUE
)

if (file.exists(postcode_filter_file)) {
  postcode_filter_raw <- data.table::fread(postcode_filter_file)

  required_columns <- c("match_key", "postcode", "keep_for_full_geocode")
  missing_required_columns <- setdiff(required_columns, names(postcode_filter_raw))
  if (length(missing_required_columns)) {
    stop(
      "Postcode filter file is missing required column(s): ",
      paste(missing_required_columns, collapse = ", ")
    )
  }

  optional_columns <- c(
    "postcode_distance_to_prison_m",
    "latitude",
    "longitude",
    "display_name",
    "geocode_status"
  )

  for (column_name in optional_columns) {
    if (!column_name %in% names(postcode_filter_raw)) {
      postcode_filter_raw[, (column_name) := NA]
    }
  }

  postcode_filter <- unique(postcode_filter_raw[, .(
    match_key,
    postcode,
    postcode_distance_to_prison_m,
    postcode_latitude = latitude,
    postcode_longitude = longitude,
    postcode_display_name = display_name,
    postcode_geocode_status = geocode_status,
    keep_for_full_geocode
  )])

  distance_lookup <- merge(
    distance_lookup,
    postcode_filter,
    by = c("match_key", "postcode"),
    all.x = TRUE
  )

  distance_lookup[, geocode_status := data.table::fcase(
    !is.na(geocode_status), geocode_status,
    !is.na(keep_for_full_geocode) & !keep_for_full_geocode, "excluded_postcode_distance_gt_threshold",
    !is.na(postcode_geocode_status) & postcode_geocode_status != "ok", paste0("postcode_", postcode_geocode_status),
    default = geocode_status
  )]
}

data.table::setnames(distance_lookup, c("ppd_id", "lmk_key"), c("unique_id", "LMK_KEY"))

postcode_distance_output <- postcode_points[, .(
  postcode,
  geocode_query,
  latitude,
  longitude,
  display_name,
  geocode_status,
  distance_to_prison_m
)]

data.table::fwrite(distance_lookup, output_file)
data.table::fwrite(postcode_distance_output, postcode_distance_output_file)

message("Distance file written to: ", output_file)
message("Postcode distance file written to: ", postcode_distance_output_file)
message("Prison location used: ", prison_location$display_name)
message("Projection used for distance: EPSG:27700")