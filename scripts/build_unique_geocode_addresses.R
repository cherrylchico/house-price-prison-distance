args <- commandArgs(trailingOnly = TRUE)

matched_ppd_file <- if (length(args) >= 1) args[[1]] else file.path("output", "matched_ppd.csv")
output_file <- if (length(args) >= 2) args[[2]] else file.path("output", "unique_address_geocode_input.csv")

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

if (!file.exists(matched_ppd_file)) {
  stop("Matched PPD file not found: ", matched_ppd_file)
}

matched_ppd <- data.table::fread(matched_ppd_file, na.strings = c("", "NA"))

required_cols <- c("unique_id", "normalized_address", "normalized_postcode")
missing_required <- setdiff(required_cols, names(matched_ppd))
if (length(missing_required)) {
  stop(
    "Matched PPD file is missing required column(s): ",
    paste(missing_required, collapse = ", "),
    ". Run scripts/build_lsoa_filtered_matched_data.R first."
  )
}

matched_ppd <- matched_ppd[
  !is.na(normalized_address) & trimws(as.character(normalized_address)) != "" &
    !is.na(normalized_postcode) & trimws(as.character(normalized_postcode)) != ""
]

matched_ppd_rows <- nrow(matched_ppd)

unique_addresses <- matched_ppd[, .(
  matched_ppd_count = .N
), by = .(normalized_address, normalized_postcode)]

data.table::setorder(unique_addresses, -matched_ppd_count, normalized_postcode, normalized_address)
unique_addresses[, unique_address_id := .I]
data.table::setcolorder(unique_addresses, c(
  "unique_address_id",
  "normalized_address",
  "normalized_postcode",
  "matched_ppd_count"
))

unique_rows <- nrow(unique_addresses)
deduplicated_rows <- matched_ppd_rows - unique_rows

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
data.table::fwrite(unique_addresses, output_file)

message("Matched PPD rows with normalized address+postcode: ", format(matched_ppd_rows, big.mark = ","))
message("Unique normalized address+postcode rows written: ", format(unique_rows, big.mark = ","))
message("Rows collapsed by deduplication: ", format(deduplicated_rows, big.mark = ","))
message("Output file written to: ", output_file)
