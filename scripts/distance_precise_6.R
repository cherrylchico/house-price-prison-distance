args <- commandArgs(trailingOnly = TRUE)

mapping_file <- if (length(args) >= 1) args[[1]] else file.path("output", "epc_ppd_mapping.csv")
geocode_file <- if (length(args) >= 2) args[[2]] else file.path("output", "address_geocodes.csv")
output_file <- if (length(args) >= 3) args[[3]] else file.path("input", "property_prison_distance.csv")
postcode_filter_file <- if (length(args) >= 4) args[[4]] else file.path("output", "postcode_prison_distance_filter.csv")
prison_shapefile_path <- if (length(args) >= 5) args[[5]] else file.path("input", "prison_shapefile", "layers", "POLYLINE.shp")

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("sf", quietly = TRUE)) {
  stop("Packages 'data.table' and 'sf' are required.")
}

if (!file.exists(mapping_file)) stop("Mapping file not found: ", mapping_file)
if (!file.exists(geocode_file)) stop("Geocode file not found: ", geocode_file)
if (!file.exists(prison_shapefile_path)) stop("Prison shapefile not found: ", prison_shapefile_path)

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

# CREATE A BACKUP OF THE 5-HOUR GEOCODE RUN
backup_file <- gsub("\\.csv$", "_backup.csv", geocode_file)
if (!file.exists(backup_file)) {
  file.copy(geocode_file, backup_file)
  message("Created safe backup of geocodes at: ", backup_file)
}

mapping <- data.table::fread(mapping_file)
address_points <- data.table::fread(geocode_file)

# ---> THE REPAIR BLOCK: Recreate geocode_status from match_type so we don't have to rerun the API <---
if (!"geocode_status" %in% names(address_points) && "match_type" %in% names(address_points)) {
  address_points[, geocode_status := data.table::fcase(
    match_type == "Exact Address", "ok",
    match_type == "Postcode Fallback", "ok_postcode_fallback",
    default = "no_result"
  )]
  message("Recovered missing 'geocode_status' column from 'match_type'.")
}

required_geocode_columns <- c(
  "match_key", "postcode", "geocode_query", 
  "match_type", "latitude", "longitude", "display_name", "geocode_status"
)
missing_geocode_columns <- setdiff(required_geocode_columns, names(address_points))
if (length(missing_geocode_columns)) {
  stop("Geocode file is missing required column(s): ", paste(missing_geocode_columns, collapse = ", "))
}

address_points <- unique(address_points, by = "match_key")
address_points[, distance_to_prison_m := NA_real_]

# FILTER OUT INCORRECT GEOCODES (POSTCODE MISMATCHES)
clean_string <- function(x) gsub("\\s+", "", toupper(x))

address_points[, is_postcode_match := mapply(function(pc, dn) {
  if (is.na(pc) || is.na(dn) || dn == "") return(FALSE)
  grepl(clean_string(pc), clean_string(dn), fixed = TRUE)
}, postcode, display_name)]

mismatch_count <- nrow(address_points[is_postcode_match == FALSE & grepl("^ok", geocode_status)])
if (mismatch_count > 0) {
  message("Found ", mismatch_count, " rows with incorrect coordinates (postcode mismatch). Excluding them from distances.")
  address_points[is_postcode_match == FALSE, `:=`(
    latitude = NA_real_, 
    longitude = NA_real_, 
    match_type = "Postcode Mismatch",
    geocode_status = "postcode_mismatch"
  )]
}

# CALCULATE DISTANCE TO PRISON SHAPEFILE
valid_points <- address_points[!is.na(latitude) & !is.na(longitude) & grepl("^ok", geocode_status)]

if (nrow(valid_points)) {
  properties_sf <- sf::st_as_sf(valid_points, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
  properties_sf <- sf::st_transform(properties_sf, 27700) 
  
  prison_sf <- sf::st_read(prison_shapefile_path, quiet = TRUE)
  prison_sf <- sf::st_transform(prison_sf, 27700)
  prison_geom <- sf::st_union(prison_sf)
  
  valid_points[, distance_to_prison_m := as.numeric(sf::st_distance(properties_sf, prison_geom))]
  
  address_points[valid_points, on = "match_key", distance_to_prison_m := i.distance_to_prison_m]
}

# PREPARE FINAL OUTPUT LOOKUP
distance_lookup <- merge(
  unique(mapping[, .(ppd_id, lmk_key, match_key, postcode)]),
  address_points[, .(match_key, geocode_query, latitude, longitude, display_name, match_type, geocode_status, distance_to_prison_m)],
  by = "match_key", all.x = TRUE
)

# POSTCODE FILTER LOGIC
if (file.exists(postcode_filter_file)) {
  postcode_filter_raw <- data.table::fread(postcode_filter_file)
  postcode_filter <- unique(postcode_filter_raw[, .(
    match_key, postcode, keep_for_full_geocode,
    postcode_distance_to_prison_m = if("postcode_distance_to_prison_m" %in% names(postcode_filter_raw)) postcode_distance_to_prison_m else NA,
    postcode_geocode_status = if("geocode_status" %in% names(postcode_filter_raw)) geocode_status else NA
  )])
  
  distance_lookup <- merge(distance_lookup, postcode_filter, by = c("match_key", "postcode"), all.x = TRUE)
  
  distance_lookup[, geocode_status := data.table::fcase(
    !is.na(geocode_status), geocode_status,
    !is.na(keep_for_full_geocode) & !keep_for_full_geocode, "excluded_postcode_distance_gt_threshold",
    !is.na(postcode_geocode_status) & postcode_geocode_status != "ok", paste0("postcode_", postcode_geocode_status),
    default = geocode_status
  )]
}

data.table::setnames(distance_lookup, c("ppd_id", "lmk_key"), c("unique_id", "LMK_KEY"))
data.table::fwrite(distance_lookup, output_file)

message("Distance mapping written to: ", output_file)
message("Distance calculation used shapefile: ", prison_shapefile_path)