args <- commandArgs(trailingOnly = TRUE)

link_file <- if (length(args) >= 1) args[[1]] else file.path("input", "ppd_link.txt")
output_file <- if (length(args) >= 2) args[[2]] else file.path("input", "ppd_data.csv")
filter_field <- if (length(args) >= 3) trimws(args[[3]]) else "county"

parse_filter_values <- function(x) {
  values <- unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)
  values <- unique(trimws(values))
  values <- values[nzchar(values)]
  toupper(values)
}

filter_values <- if (length(args) >= 4) parse_filter_values(args[[4]]) else "STAFFORD"

if (!nzchar(filter_field)) {
  stop("Filter field must be provided.")
}

if (!length(filter_values)) {
  stop("At least one filter value must be provided.")
}

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

if (!file.exists(link_file)) {
  stop("Link file not found: ", link_file)
}

# Increase download timeout for large remote CSV files.
options(timeout = max(600, getOption("timeout")))

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
  " PPD file(s) and filtering ",
  filter_field,
  " in {",
  paste(filter_values, collapse = ", "),
  "}."
)

read_ppd_with_retry <- function(link, ppd_columns, max_attempts = 3) {
  last_error <- NULL

  for (attempt in seq_len(max_attempts)) {
    out <- tryCatch(
      data.table::fread(
        link,
        header = FALSE,
        col.names = ppd_columns,
        na.strings = c("", "NA")
      ),
      error = function(e) {
        last_error <<- e
        NULL
      }
    )

    if (!is.null(out)) {
      if (attempt > 1) {
        message("Succeeded after retry (attempt ", attempt, "): ", link)
      }
      return(out)
    }

    if (attempt < max_attempts) {
      message(
        "Attempt ", attempt, " failed for ", link,
        "; retrying in 3 seconds..."
      )
      Sys.sleep(3)
    }
  }

  stop(
    "Failed to read PPD URL after ", max_attempts, " attempts: ",
    link,
    if (!is.null(last_error)) paste0("\nLast error: ", conditionMessage(last_error)) else ""
  )
}

filtered_tables <- lapply(links, function(link) {
  message("Processing: ", link)

  dt <- read_ppd_with_retry(link, ppd_columns)

  if (!(filter_field %in% names(dt))) {
    stop("Filter field not found in PPD data: ", filter_field)
  }

  field_values <- toupper(trimws(as.character(dt[[filter_field]])))
  dt[field_values %in% filter_values]
})

filtered_ppd <- data.table::rbindlist(filtered_tables, use.names = TRUE, fill = TRUE)

data.table::fwrite(filtered_ppd, output_file)

message("Filtered PPD rows written to: ", output_file)
message("Rows written: ", format(nrow(filtered_ppd), big.mark = ","))