args <- commandArgs(trailingOnly = TRUE)

mapping_file <- if (length(args) >= 1) args[[1]] else file.path("output", "epc_ppd_mapping.csv")
output_file <- if (length(args) >= 2) args[[2]] else file.path("output", "postcode_prison_distance_filter.csv")
postcode_lookup_file <- if (length(args) >= 3) args[[3]] else file.path("output", "postcode_lsoa_lookup_target_lads.csv")
lsoa_boundary_file <- if (length(args) >= 4) args[[4]] else file.path("input", "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW.geojson")
max_distance_km <- if (length(args) >= 5) as.numeric(args[[5]]) else 11
filtered_output_file <- if (length(args) >= 6) args[[6]] else file.path("output", "postcode_prison_distance_candidates.csv")
selected_lsoa_file <- if (length(args) >= 7) args[[7]] else file.path("output", "fosse_way_lsoa_bbox_candidates.geojson")
summary_file <- if (length(args) >= 8) args[[8]] else file.path("output", "postcode_prison_distance_filter_summary.txt")

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("sf", quietly = TRUE)) {
  stop("Packages 'data.table' and 'sf' are required.")
}

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
}

if (!file.exists(postcode_lookup_file)) {
  stop("Postcode lookup file not found: ", postcode_lookup_file)
}

if (!file.exists(lsoa_boundary_file)) {
  stop("LSOA boundary file not found: ", lsoa_boundary_file)
}

if (is.na(max_distance_km) || max_distance_km <= 0) {
  stop("max_distance_km must be a positive number.")
}

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(filtered_output_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(selected_lsoa_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(summary_file), recursive = TRUE, showWarnings = FALSE)

normalize_postcode <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(gsub("\\s+", "", x))
  trimws(x)
}

center_lat <- 52.584126
center_lon <- -1.145212
lat_delta <- max_distance_km / 111.32
lon_delta <- max_distance_km / (111.32 * cos(center_lat * pi / 180))

bbox_polygon <- sf::st_as_sfc(
  sf::st_bbox(
    c(
      xmin = center_lon - lon_delta,
      ymin = center_lat - lat_delta,
      xmax = center_lon + lon_delta,
      ymax = center_lat + lat_delta
    ),
    crs = sf::st_crs(4326)
  )
)

mapping <- data.table::fread(mapping_file)

postcode_keys <- unique(mapping[, .(
  match_key,
  postcode
)])[!is.na(match_key) & !is.na(postcode)]
postcode_keys[, postcode_key := normalize_postcode(postcode)]
postcode_keys <- postcode_keys[nzchar(postcode_key)]

postcode_lookup <- data.table::fread(postcode_lookup_file, na.strings = c("", "NA"))

if (!"postcode_key" %in% names(postcode_lookup)) {
  if (!"pcd7" %in% names(postcode_lookup)) {
    stop("Postcode lookup file must contain either 'postcode_key' or 'pcd7'.")
  }
  postcode_lookup[, postcode_key := normalize_postcode(pcd7)]
}

if (!"lsoa21cd" %in% names(postcode_lookup)) {
  stop("Postcode lookup file must contain 'lsoa21cd'.")
}

if (!"ladcd" %in% names(postcode_lookup)) {
  postcode_lookup[, ladcd := NA_character_]
}

if (!"ladnm" %in% names(postcode_lookup)) {
  postcode_lookup[, ladnm := NA_character_]
}

postcode_lookup <- postcode_lookup[
  nzchar(postcode_key),
  .(postcode_key, lsoa21cd, ladcd, ladnm)
]

postcode_lookup <- postcode_lookup[order(postcode_key, -nzchar(lsoa21cd))]
postcode_lookup <- postcode_lookup[, .SD[1], by = postcode_key]

message("Reading LSOA boundaries and selecting polygons that intersect the bounding box.")

lsoa_boundaries <- sf::st_read(lsoa_boundary_file, quiet = TRUE)

previous_s2 <- sf::sf_use_s2(FALSE)
on.exit(sf::sf_use_s2(previous_s2), add = TRUE)

if (is.na(sf::st_crs(lsoa_boundaries))) {
  sf::st_crs(lsoa_boundaries) <- sf::st_crs(4326)
}

lsoa_boundaries <- sf::st_transform(lsoa_boundaries, 4326)

if (!"LSOA21CD" %in% names(lsoa_boundaries)) {
  stop("LSOA boundary file must contain 'LSOA21CD'.")
}

selected_lsoas <- lsoa_boundaries[sf::st_intersects(lsoa_boundaries, bbox_polygon, sparse = FALSE)[, 1], ]

if (!nrow(selected_lsoas)) {
  stop("No LSOA boundaries intersect the requested bounding box.")
}

sf::st_write(selected_lsoas, selected_lsoa_file, delete_dsn = TRUE, quiet = TRUE)

selected_lsoa_codes <- unique(selected_lsoas$LSOA21CD)

postcode_points <- merge(
  postcode_keys,
  postcode_lookup,
  by = "postcode_key",
  all.x = TRUE
)

postcode_points[, keep_for_full_geocode := !is.na(lsoa21cd) & lsoa21cd %in% selected_lsoa_codes]

postcode_candidates <- postcode_points[keep_for_full_geocode == TRUE]

data.table::fwrite(postcode_points, output_file)
data.table::fwrite(postcode_candidates, filtered_output_file)

summary_lines <- c(
  paste("Bounding box center latitude:", format(center_lat, digits = 8)),
  paste("Bounding box center longitude:", format(center_lon, digits = 8)),
  paste("Bounding box half-width/height (km):", format(max_distance_km, digits = 6)),
  paste("Bounding box min latitude:", format(center_lat - lat_delta, digits = 8)),
  paste("Bounding box max latitude:", format(center_lat + lat_delta, digits = 8)),
  paste("Bounding box min longitude:", format(center_lon - lon_delta, digits = 8)),
  paste("Bounding box max longitude:", format(center_lon + lon_delta, digits = 8)),
  paste("Unique property keys reviewed:", format(nrow(postcode_points), big.mark = ",")),
  paste("Unique postcodes reviewed:", format(data.table::uniqueN(postcode_points$postcode), big.mark = ",")),
  paste("Distinct selected LSOAs intersecting bbox:", format(length(selected_lsoa_codes), big.mark = ",")),
  paste("Postcodes with postcode-to-LSOA match:", format(sum(!is.na(postcode_points$lsoa21cd) & nzchar(postcode_points$lsoa21cd)), big.mark = ",")),
  paste("Unique postcodes retained via LSOA bbox filter:", format(data.table::uniqueN(postcode_candidates$postcode), big.mark = ",")),
  paste("Retained via LSOA bbox filter:", format(nrow(postcode_candidates), big.mark = ",")),
  paste("Excluded by LSOA bbox filter:", format(nrow(postcode_points) - nrow(postcode_candidates), big.mark = ","))
)
writeLines(summary_lines, summary_file)

message("Postcode filter written to: ", output_file)
message("Postcode candidates written to: ", filtered_output_file)
message("Selected LSOA boundaries written to: ", selected_lsoa_file)
message("Summary written to: ", summary_file)
message("Unique property keys reviewed: ", format(nrow(postcode_points), big.mark = ","))
message("Distinct selected LSOAs intersecting bbox: ", format(length(selected_lsoa_codes), big.mark = ","))
message("Retained via LSOA bbox filter: ", format(nrow(postcode_candidates), big.mark = ","))