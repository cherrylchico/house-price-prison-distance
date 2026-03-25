args <- commandArgs(trailingOnly = TRUE)

input_geojson <- if (length(args) >= 1) args[[1]] else NA_character_
pad_meters <- if (length(args) >= 2) suppressWarnings(as.numeric(args[[2]])) else NA_real_
output_geojson <- if (length(args) >= 3) args[[3]] else NA_character_

if (is.na(input_geojson) || !nzchar(input_geojson)) {
  stop("Usage: Rscript scripts/create_bbox_27700.R <input_geojson> <pad_meters> [output_geojson]")
}

if (is.na(pad_meters) || !is.finite(pad_meters) || pad_meters < 0) {
  stop("'pad_meters' must be a non-negative numeric value.")
}

if (!requireNamespace("sf", quietly = TRUE)) {
  stop("Package 'sf' is required. Install it with install.packages('sf').")
}

if (!file.exists(input_geojson)) {
  stop("Input GeoJSON not found: ", input_geojson)
}

x <- sf::st_read(input_geojson, quiet = TRUE)

if (!nrow(x)) {
  stop("Input GeoJSON has no features: ", input_geojson)
}

x_27700 <- sf::st_transform(x, 27700)
base_bbox <- sf::st_bbox(x_27700)

bbox_27700 <- sf::st_bbox(c(
  xmin = unname(base_bbox["xmin"]) - pad_meters,
  ymin = unname(base_bbox["ymin"]) - pad_meters,
  xmax = unname(base_bbox["xmax"]) + pad_meters,
  ymax = unname(base_bbox["ymax"]) + pad_meters
), crs = sf::st_crs(27700))

bbox_poly <- sf::st_as_sfc(bbox_27700)
bbox_4326 <- sf::st_bbox(sf::st_transform(bbox_poly, 4326))

cat("Bounding box (EPSG:27700)\n")
cat("xmin:", format(unname(bbox_27700["xmin"]), scientific = FALSE), "\n")
cat("ymin:", format(unname(bbox_27700["ymin"]), scientific = FALSE), "\n")
cat("xmax:", format(unname(bbox_27700["xmax"]), scientific = FALSE), "\n")
cat("ymax:", format(unname(bbox_27700["ymax"]), scientific = FALSE), "\n")
cat("pad_meters:", format(pad_meters, scientific = FALSE), "\n")

cat("\nBounding box (EPSG:4326, lon/lat)\n")
cat("min_lon:", format(unname(bbox_4326["xmin"]), scientific = FALSE, digits = 10), "\n")
cat("min_lat:", format(unname(bbox_4326["ymin"]), scientific = FALSE, digits = 10), "\n")
cat("max_lon:", format(unname(bbox_4326["xmax"]), scientific = FALSE, digits = 10), "\n")
cat("max_lat:", format(unname(bbox_4326["ymax"]), scientific = FALSE, digits = 10), "\n")

if (!is.na(output_geojson) && nzchar(output_geojson)) {
  sf::st_write(sf::st_sf(geometry = bbox_poly), output_geojson, delete_dsn = TRUE, quiet = TRUE)
  cat("Wrote bbox polygon to:", output_geojson, "\n")
}
