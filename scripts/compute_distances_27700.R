args <- commandArgs(trailingOnly = TRUE)

target_location <- if (length(args) >= 1) args[[1]] else file.path("input", "landfill.geojson")
address_files_pattern <- if (length(args) >= 2) args[[2]] else file.path("output", "address_geocodes*")
output_file <- if (length(args) >= 3) args[[3]] else file.path("output", "address_landfill_distances_27700.csv")

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("sf", quietly = TRUE)) {
  stop("Packages 'data.table' and 'sf' are required.")
}

if (!file.exists(target_location)) {
  stop("Target location file not found: ", target_location)
}

resolve_address_files <- function(pattern_or_prefix) {
  x <- trimws(as.character(pattern_or_prefix))
  if (!nzchar(x)) {
    return(character())
  }

  has_wildcard <- grepl("[*?[]", x)
  if (has_wildcard) {
    files <- Sys.glob(x)
  } else {
    files <- Sys.glob(paste0(x, "*"))
  }

  files <- files[file.exists(files)]
  files <- files[!dir.exists(files)]
  sort(unique(files))
}

address_files <- resolve_address_files(address_files_pattern)
if (!length(address_files)) {
  stop("No address geocode files matched pattern/prefix: ", address_files_pattern)
}

message("Address files matched: ", length(address_files))
for (f in address_files) {
  message(" - ", f)
}

address_tables <- lapply(address_files, function(path) {
  dt <- data.table::fread(path, na.strings = c("", "NA"))
  dt[, source_file := basename(path)]
  dt
})

address_dt <- data.table::rbindlist(address_tables, use.names = TRUE, fill = TRUE)
if (!nrow(address_dt)) {
  stop("Address geocode inputs are empty after reading matched files.")
}

required_cols <- c("latitude", "longitude")
missing_cols <- setdiff(required_cols, names(address_dt))
if (length(missing_cols)) {
  stop("Address geocode data is missing required column(s): ", paste(missing_cols, collapse = ", "))
}

address_dt[, latitude := suppressWarnings(as.numeric(latitude))]
address_dt[, longitude := suppressWarnings(as.numeric(longitude))]

address_dt <- address_dt[!is.na(latitude) & !is.na(longitude)]
if (!nrow(address_dt)) {
  stop("No valid latitude/longitude rows found in matched address files.")
}

target_sf <- sf::st_read(target_location, quiet = TRUE)
if (!nrow(target_sf)) {
  stop("Target location GeoJSON contains no features: ", target_location)
}

if (is.na(sf::st_crs(target_sf))) {
  stop("Target location has no CRS. Please provide CRS metadata in: ", target_location)
}

address_sf <- sf::st_as_sf(address_dt, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
address_sf_27700 <- sf::st_transform(address_sf, 27700)
target_sf_27700 <- sf::st_transform(target_sf, 27700)

dist_matrix <- sf::st_distance(address_sf_27700, target_sf_27700)
min_dist <- apply(as.matrix(dist_matrix), 1, min)
nearest_idx <- apply(as.matrix(dist_matrix), 1, which.min)

address_dt[, distance_to_target_m := as.numeric(min_dist)]
address_dt[, nearest_target_feature_index := as.integer(nearest_idx)]
address_dt[, distance_crs_epsg := 27700L]

out_cols <- c(
  intersect(c(
    "source_file",
    "unique_address_id",
    "normalized_address",
    "normalized_postcode",
    "geocode_query",
    "match_type",
    "latitude",
    "longitude",
    "display_name"
  ), names(address_dt)),
  "distance_to_target_m",
  "nearest_target_feature_index",
  "distance_crs_epsg"
)

output_dt <- address_dt[, ..out_cols]

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
data.table::fwrite(output_dt, output_file)

message("Rows in input addresses with valid coordinates: ", format(nrow(address_dt), big.mark = ","))
message("Rows written to output: ", format(nrow(output_dt), big.mark = ","))
message("Distance units: meters (EPSG:27700)")
message("Output written to: ", output_file)
