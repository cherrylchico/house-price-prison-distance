args <- commandArgs(trailingOnly = TRUE)

input_dir <- if (length(args) >= 1) args[[1]] else "input"
mapping_file <- if (length(args) >= 2) args[[2]] else file.path("output", "epc_ppd_mapping.csv")
output_dir <- if (length(args) >= 3) args[[3]] else "output"
postcode_lookup_file <- if (length(args) >= 4) args[[4]] else file.path("input", "PCD_OA21_LSOA21_MSOA21_LAD_AUG24_UK_LU.csv")
target_codes_geojson <- if (length(args) >= 5) args[[5]] else file.path(input_dir, "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW.geojson")
summary_file <- if (length(args) >= 6) args[[6]] else file.path(output_dir, "build_lsoa_filtered_matched_data_summary.txt")

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

if (!requireNamespace("sf", quietly = TRUE)) {
  stop("Package 'sf' is required. Install it with install.packages('sf').")
}

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
}

if (!file.exists(postcode_lookup_file)) {
  stop("Postcode lookup file not found: ", postcode_lookup_file)
}

if (!file.exists(target_codes_geojson)) {
  stop("Target codes GeoJSON not found: ", target_codes_geojson)
}

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

mapping <- data.table::fread(mapping_file)

if (!nrow(mapping)) {
  stop("The mapping file exists but contains no matched rows.")
}

message("Rows in mapping: ", format(nrow(mapping), big.mark = ","))
mapping_rows <- nrow(mapping)
mapping_unique_lmk <- data.table::uniqueN(mapping$lmk_key)
mapping_unique_ppd <- data.table::uniqueN(mapping$ppd_id)

normalize_postcode <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(gsub("\\s+", "", x))
  trimws(x)
}

normalize_text <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(trimws(x))
  x <- gsub("&", " AND ", x, fixed = TRUE)
  x <- gsub("[^A-Z0-9]+", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

target_codes_sf <- sf::st_read(target_codes_geojson, quiet = TRUE)
code_col <- if ("LSOA21CD" %in% names(target_codes_sf)) {
  "LSOA21CD"
} else if ("lsoa21cd" %in% names(target_codes_sf)) {
  "lsoa21cd"
} else {
  stop(
    "Target codes GeoJSON must contain an 'LSOA21CD' column. Available columns: ",
    paste(names(target_codes_sf), collapse = ", ")
  )
}

target_lsoa_codes <- unique(toupper(trimws(as.character(target_codes_sf[[code_col]]))))
target_lsoa_codes <- target_lsoa_codes[nzchar(target_lsoa_codes)]
target_geojson_rows <- nrow(target_codes_sf)
target_lsoa_count <- length(target_lsoa_codes)

if (!length(target_lsoa_codes)) {
  stop("No LSOA codes found in GeoJSON column '", code_col, "': ", target_codes_geojson)
}

message("Reading postcode lookup and filtering to target LSOA codes from GeoJSON: ", target_codes_geojson, " (column: ", code_col, ")")

postcode_lookup <- data.table::fread(
  postcode_lookup_file,
  select = c("pcd7", "lsoa21cd"),
  na.strings = c("", "NA")
)

postcode_lookup_rows_raw <- nrow(postcode_lookup)
postcode_lookup_missing_lsoa_raw <- sum(is.na(postcode_lookup$lsoa21cd) | !nzchar(trimws(as.character(postcode_lookup$lsoa21cd))))
postcode_lookup_nonmissing_lsoa_raw <- postcode_lookup_rows_raw - postcode_lookup_missing_lsoa_raw

postcode_lookup[, postcode_key := normalize_postcode(pcd7)]
postcode_lookup <- postcode_lookup[nzchar(postcode_key), .(postcode_key, lsoa21cd)]

postcode_lookup <- postcode_lookup[order(postcode_key, -nzchar(lsoa21cd))]
postcode_lookup <- postcode_lookup[, .SD[1], by = postcode_key]
postcode_lookup_rows_deduped <- nrow(postcode_lookup)
postcode_lookup_unique_postcodes <- data.table::uniqueN(postcode_lookup$postcode_key)

message("Loading matched EPC certificates.")

epc_keys <- unique(mapping[, .(epc_source, lmk_key)])
epc_sources <- unique(epc_keys$epc_source)

epc_tables <- lapply(epc_sources, function(source_name) {
  file_path <- file.path(input_dir, source_name, "certificates.csv")
  dt <- data.table::fread(file_path, na.strings = c("", "NA", "NODATA!", "NO DATA!"))
  wanted <- epc_keys[epc_source == source_name, lmk_key]
  dt <- dt[LMK_KEY %chin% wanted]
  dt[, epc_source := source_name]
  dt
})

matched_epc <- data.table::rbindlist(epc_tables, use.names = TRUE, fill = TRUE)
matched_epc_rows <- nrow(matched_epc)
message("Matched EPC rows (no LSOA processing applied): ", format(matched_epc_rows, big.mark = ","))

message("Loading matched PPD transactions.")

ppd_ids <- unique(mapping$ppd_id)
ppd_file <- file.path(input_dir, "ppd_data.csv")
matched_ppd <- data.table::fread(ppd_file, na.strings = c("", "NA"))
matched_ppd <- matched_ppd[unique_id %chin% ppd_ids]
matched_ppd[, postcode_key := normalize_postcode(postcode)]

matched_ppd_rows_pre_attach <- nrow(matched_ppd)

matched_ppd <- merge(matched_ppd, postcode_lookup[, .(postcode_key, lsoa21cd)], by = "postcode_key", all.x = TRUE)

missing_lsoa_mask <- is.na(matched_ppd$lsoa21cd) | !nzchar(trimws(as.character(matched_ppd$lsoa21cd)))
ppd_postcodes_missing_lsoa_count <- data.table::uniqueN(matched_ppd[missing_lsoa_mask, postcode])

matched_ppd_rows_pre_keep <- nrow(matched_ppd)
matched_ppd_missing_lsoa <- sum(is.na(matched_ppd$lsoa21cd) | !nzchar(trimws(as.character(matched_ppd$lsoa21cd))))
matched_ppd_nonmissing_lsoa <- matched_ppd_rows_pre_keep - matched_ppd_missing_lsoa

matched_ppd <- matched_ppd[!missing_lsoa_mask]
matched_ppd <- matched_ppd[toupper(trimws(as.character(lsoa21cd))) %in% target_lsoa_codes]
matched_ppd[, normalized_address := normalize_text(paste(saon, paon, street))]
matched_ppd[, normalized_postcode := normalize_postcode(postcode)]
matched_ppd[, postcode_key := NULL]
matched_ppd_rows <- nrow(matched_ppd)

matched_epc_file <- file.path(output_dir, "matched_epc.csv")
matched_ppd_file <- file.path(output_dir, "matched_ppd.csv")

data.table::fwrite(matched_epc, matched_epc_file)
data.table::fwrite(matched_ppd, matched_ppd_file)

dir.create(dirname(summary_file), recursive = TRUE, showWarnings = FALSE)
summary_lines <- c(
  paste("Mapping rows (input):", format(mapping_rows, big.mark = ",")),
  paste("Distinct LMK_KEY in mapping:", format(mapping_unique_lmk, big.mark = ",")),
  paste("Distinct PPD IDs in mapping:", format(mapping_unique_ppd, big.mark = ",")),
  paste("Target GeoJSON features:", format(target_geojson_rows, big.mark = ",")),
  paste("Distinct target LSOA21CD values:", format(target_lsoa_count, big.mark = ",")),
  paste("Postcode lookup rows (input):", format(postcode_lookup_rows_raw, big.mark = ",")),
  paste("Postcode lookup rows with missing lsoa21cd (input, pre-match):", format(postcode_lookup_missing_lsoa_raw, big.mark = ",")),
  paste("Postcode lookup rows with non-missing lsoa21cd (input, pre-match):", format(postcode_lookup_nonmissing_lsoa_raw, big.mark = ",")),
  paste("Postcode lookup rows (deduped by postcode_key):", format(postcode_lookup_rows_deduped, big.mark = ",")),
  paste("Distinct postcode_key in lookup:", format(postcode_lookup_unique_postcodes, big.mark = ",")),
  paste("Matched EPC rows (output, no LSOA processing):", format(matched_epc_rows, big.mark = ",")),
  paste("Matched PPD rows (pre-attach):", format(matched_ppd_rows_pre_attach, big.mark = ",")),
  paste("Matched PPD rows (pre-keep):", format(matched_ppd_rows_pre_keep, big.mark = ",")),
  paste("Matched PPD rows with missing lsoa21cd (pre-keep):", format(matched_ppd_missing_lsoa, big.mark = ",")),
  paste("Matched PPD rows with non-missing lsoa21cd (pre-keep):", format(matched_ppd_nonmissing_lsoa, big.mark = ",")),
  paste("PPD postcodes without attached lsoa21cd:", format(ppd_postcodes_missing_lsoa_count, big.mark = ",")),
  paste("Matched PPD rows kept (non-missing and within target LSOA21CD):", format(matched_ppd_rows, big.mark = ","))
)
writeLines(summary_lines, summary_file)

message("Matched EPC written to: ", matched_epc_file)
message("Matched PPD written to: ", matched_ppd_file)
message("Summary written to: ", summary_file)
