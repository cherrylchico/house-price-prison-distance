args <- commandArgs(trailingOnly = TRUE)

input_dir <- if (length(args) >= 1) args[[1]] else "input"
mapping_file <- if (length(args) >= 2) args[[2]] else file.path("output", "epc_ppd_mapping.csv")
output_dir <- if (length(args) >= 3) args[[3]] else "output"
postcode_lookup_file <- if (length(args) >= 4) args[[4]] else file.path("input", "PCD_OA21_LSOA21_MSOA21_LAD_AUG24_UK_LU.csv")

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
}

if (!file.exists(postcode_lookup_file)) {
  stop("Postcode lookup file not found: ", postcode_lookup_file)
}

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

mapping <- data.table::fread(mapping_file)

if (!nrow(mapping)) {
  stop("The mapping file exists but contains no matched rows.")
}

exclude_districts <- toupper(c("NORTH WEST LEICESTERSHIRE", "MELTON"))
mapping <- mapping[!toupper(trimws(district)) %in% exclude_districts]
message("Rows in mapping after district exclusion: ", format(nrow(mapping), big.mark = ","))

normalize_postcode <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(gsub("\\s+", "", x))
  trimws(x)
}

target_lads <- c(
  "E06000016",
  "E07000130",
  "E07000131",
  "E07000135",
  "E07000129",
  "E07000132",
  "E07000133",
  "E07000134"
)


message("Reading postcode lookup and filtering to target LADs.")

postcode_lookup <- data.table::fread(
  postcode_lookup_file,
  select = c("pcd7", "lsoa21cd", "ladcd","ladnm"),
  na.strings = c("", "NA")
)

postcode_lookup <- postcode_lookup[toupper(trimws(ladcd)) %in% toupper(target_lads)]
postcode_lookup[, postcode_key := normalize_postcode(pcd7)]
postcode_lookup <- postcode_lookup[nzchar(postcode_key), .(postcode_key, lsoa21cd, ladcd, ladnm)]

postcode_lookup <- postcode_lookup[order(postcode_key, -nzchar(lsoa21cd))]
postcode_lookup <- postcode_lookup[, .SD[1], by = postcode_key]

lad_lookup_file <- file.path(output_dir, "postcode_lsoa_lookup_target_lads.csv")
data.table::fwrite(postcode_lookup, lad_lookup_file)
message("Filtered postcode lookup written to: ", lad_lookup_file)

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
matched_epc[, postcode_key := normalize_postcode(POSTCODE)]
matched_epc <- merge(matched_epc, postcode_lookup[, .(postcode_key, lsoa21cd)], by = "postcode_key", all.x = TRUE)
matched_epc[, postcode_key := NULL]

message("Loading matched PPD transactions.")

ppd_ids <- unique(mapping$ppd_id)
ppd_file <- file.path(input_dir, "ppd_data.csv")
matched_ppd <- data.table::fread(ppd_file, na.strings = c("", "NA"))
matched_ppd <- matched_ppd[unique_id %chin% ppd_ids]
matched_ppd[, postcode_key := normalize_postcode(postcode)]
matched_ppd <- merge(matched_ppd, postcode_lookup[, .(postcode_key, lsoa21cd)], by = "postcode_key", all.x = TRUE)
matched_ppd[, postcode_key := NULL]

matched_epc_file <- file.path(output_dir, "matched_epc.csv")
matched_ppd_file <- file.path(output_dir, "matched_ppd.csv")

data.table::fwrite(matched_epc, matched_epc_file)
data.table::fwrite(matched_ppd, matched_ppd_file)

message("Matched EPC written to: ", matched_epc_file)
message("Matched PPD written to: ", matched_ppd_file)
