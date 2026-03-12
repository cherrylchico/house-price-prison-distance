args <- commandArgs(trailingOnly = TRUE)

link_file <- if (length(args) >= 1) args[[1]] else file.path("input", "ppd_link.txt")
output_file <- if (length(args) >= 2) args[[2]] else file.path("input", "ppd_data.csv")
target_counties <- if (length(args) >= 3) args[3:length(args)] else "LEICESTERSHIRE"
target_counties <- unlist(strsplit(target_counties, ",", fixed = TRUE), use.names = FALSE)
target_counties <- unique(trimws(target_counties))
target_counties <- target_counties[nzchar(target_counties)]
target_counties <- toupper(target_counties)

if (!length(target_counties)) {
  stop("At least one county must be provided.")
}

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

if (!file.exists(link_file)) {
  stop("Link file not found: ", link_file)
}

ppd_columns <- c(
  "unique_id",
  "price_paid",
  "deed_date",
  "postcode",
  "property_type",
  "new_build",
  "estate_type",
  "saon",
  "paon",
  "street",
  "locality",
  "town",
  "district",
  "county",
  "transaction_category",
  "linked_data_uri"
)

links <- readLines(link_file, warn = FALSE)
links <- trimws(links)
links <- links[nzchar(links)]

if (!length(links)) {
  stop("No links found in: ", link_file)
}

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

message(
  "Reading ",
  length(links),
  " PPD file(s) and filtering county in {",
  paste(target_counties, collapse = ", "),
  "}."
)

filtered_tables <- lapply(links, function(link) {
  message("Processing: ", link)

  dt <- data.table::fread(
    link,
    header = FALSE,
    col.names = ppd_columns,
    na.strings = c("", "NA")
  )

  dt[toupper(trimws(county)) %in% target_counties]
})

filtered_ppd <- data.table::rbindlist(filtered_tables, use.names = TRUE, fill = TRUE)

data.table::fwrite(filtered_ppd, output_file)

message("Filtered PPD rows written to: ", output_file)
message("Rows written: ", format(nrow(filtered_ppd), big.mark = ","))