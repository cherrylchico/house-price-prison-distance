args <- commandArgs(trailingOnly = TRUE)

input_dir <- if (length(args) >= 1) args[[1]] else "input"
mapping_file <- if (length(args) >= 2) args[[2]] else file.path("output", "epc_ppd_mapping.csv")
output_dir <- if (length(args) >= 3) args[[3]] else "output"

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
}

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

mapping <- data.table::fread(mapping_file)

if (!nrow(mapping)) {
  stop("The mapping file exists but contains no matched rows.")
}

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

message("Loading matched PPD transactions.")

ppd_ids <- unique(mapping$ppd_id)
ppd_file <- file.path(input_dir, "ppd_data.csv")
matched_ppd <- data.table::fread(ppd_file, na.strings = c("", "NA"))
matched_ppd <- matched_ppd[unique_id %chin% ppd_ids]

matched_epc_file <- file.path(output_dir, "matched_epc.csv")
matched_ppd_file <- file.path(output_dir, "matched_ppd.csv")

data.table::fwrite(matched_epc, matched_epc_file)
data.table::fwrite(matched_ppd, matched_ppd_file)

message("Matched EPC written to: ", matched_epc_file)
message("Matched PPD written to: ", matched_ppd_file)
