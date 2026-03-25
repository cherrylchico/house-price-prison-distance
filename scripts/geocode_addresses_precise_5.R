args <- commandArgs(trailingOnly = TRUE)

input_file <- if (length(args) >= 1) args[[1]] else file.path("output", "unique_address_geocode_input.csv")
output_file <- if (length(args) >= 2) args[[2]] else file.path("output", "address_geocodes.csv")
cache_file <- if (length(args) >= 3) args[[3]] else file.path("output", "address_geocode_cache.csv")
api_key <- if (length(args) >= 4) args[[4]] else Sys.getenv("MAPBOX_API_KEY")
row_range_arg <- if (length(args) >= 5) args[[5]] else ""

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("jsonlite", quietly = TRUE) ||
    !requireNamespace("curl", quietly = TRUE)) {
  stop("Packages 'data.table', 'jsonlite', and 'curl' are required.")
}

if (is.null(api_key) || !nzchar(trimws(api_key))) {
  stop("Mapbox API key is required as argument 4 or via MAPBOX_API_KEY environment variable.")
}

parse_row_range <- function(x) {
  x <- trimws(as.character(x))
  if (!nzchar(x)) {
    return(NULL)
  }

if (!grepl("^[0-9]+\\s*-\\s*[0-9]+$", x)) {
    stop("Row range must be in the format start-end, for example 0-1000.")
  }

  parts <- strsplit(gsub("\\s+", "", x), "-", fixed = TRUE)[[1]]
  start <- as.integer(parts[[1]])
  end <- as.integer(parts[[2]])

  if (is.na(start) || is.na(end) || end < start) {
    stop("Invalid row range: end must be greater than or equal to start.")
  }

  list(start = start, end = end, label = paste0(start, "-", end))
}

apply_output_suffix <- function(path, suffix) {
  ext <- tools::file_ext(path)
  has_ext <- nzchar(ext)
  stem <- if (has_ext) {
    sub(paste0("\\.", ext, "$"), "", path)
  } else {
    path
  }

  if (has_ext) {
    paste0(stem, "_", suffix, ".", ext)
  } else {
    paste0(stem, "_", suffix)
  }
}

row_range <- parse_row_range(row_range_arg)
if (!is.null(row_range)) {
  output_file <- apply_output_suffix(output_file, row_range$label)
}

if (!file.exists(input_file)) {
  stop("Unique geocode input file not found: ", input_file)
}

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cache_file), recursive = TRUE, showWarnings = FALSE)

# Mapbox fetcher function with fallback support
fetch_geocode <- function(query, fallback_query = NULL, api_key, pause_s = 0.1, retries = 3) {
  encoded_query <- utils::URLencode(query, reserved = TRUE)
  url <- sprintf("https://api.mapbox.com/geocoding/v5/mapbox.places/%s.json?access_token=%s&country=gb&limit=1", encoded_query, api_key)
  last_error <- NULL
  
  for (attempt in seq_len(retries)) {
    response <- tryCatch(curl::curl_fetch_memory(url), error = function(e) e)
    
    if (!inherits(response, "error")) {
      status_code <- response$status_code
      body <- rawToChar(response$content)
      
      if (status_code >= 200 && status_code < 300) {
        parsed <- jsonlite::fromJSON(body, simplifyDataFrame = TRUE)
        features <- parsed$features
        
        if (is.null(features) || length(features) == 0 || (is.data.frame(features) && nrow(features) == 0)) {
          Sys.sleep(pause_s)
          if (!is.null(fallback_query) && !is.na(fallback_query)) {
            fallback_res <- fetch_geocode(query = fallback_query, fallback_query = NULL, api_key = api_key, pause_s = pause_s, retries = retries)
            if (fallback_res$geocode_status == "ok") fallback_res$geocode_status <- "ok_postcode_fallback"
            return(fallback_res)
          }
          return(list(latitude = NA_real_, longitude = NA_real_, display_name = NA_character_, geocode_status = "no_result"))
        }
        
        lon <- features$center[[1]][1]
        lat <- features$center[[1]][2]
        display_name <- features$place_name[1]
        
        Sys.sleep(pause_s)
        return(list(latitude = as.numeric(lat), longitude = as.numeric(lon), display_name = as.character(display_name), geocode_status = "ok"))
      }
      last_error <- sprintf("HTTP %s: %s", status_code, body)
    } else {
      last_error <- conditionMessage(response)
    }
    Sys.sleep(attempt) 
  }
  list(latitude = NA_real_, longitude = NA_real_, display_name = NA_character_, geocode_status = paste("error", last_error))
}

# 1. Load unique normalized addresses
address_points <- data.table::fread(input_file, na.strings = c("", "NA"))

required_cols <- c("normalized_address", "normalized_postcode")
missing_cols <- setdiff(required_cols, names(address_points))
if (length(missing_cols)) {
  stop(
    "Input file is missing required column(s): ",
    paste(missing_cols, collapse = ", "),
    ". Expected columns include normalized_address and normalized_postcode."
  )
}

address_points <- address_points[
  !is.na(normalized_address) & trimws(as.character(normalized_address)) != "" &
    !is.na(normalized_postcode) & trimws(as.character(normalized_postcode)) != ""
]

address_points[, row_index0 := .I - 1L]

if (!is.null(row_range)) {
  total_rows <- nrow(address_points)
  address_points <- address_points[row_index0 >= row_range$start & row_index0 <= row_range$end]
  message(
    "Row range ", row_range$label, " retained ",
    format(nrow(address_points), big.mark = ","),
    " of ", format(total_rows, big.mark = ","),
    " input rows for geocoding."
  )
}

address_points[, geocode_query := paste(normalized_address, normalized_postcode, sep = ", ")]
address_points[, fallback_query := normalized_postcode]

if (file.exists(cache_file)) {
  geocode_cache <- data.table::fread(cache_file)
} else {
  geocode_cache <- data.table::data.table(
    query = character(), latitude = numeric(), longitude = numeric(),
    display_name = character(), geocode_status = character()
  )
}

needed_queries <- unique(address_points[, .(geocode_query, fallback_query)])
missing_queries <- needed_queries[!geocode_query %in% geocode_cache$query]

if (nrow(missing_queries) > 0) {
  message("Geocoding ", format(nrow(missing_queries), big.mark = ","), " new address queries (with postcode fallback enabled).")
} else {
  message("All addresses are already cached. Skipping API calls.")
}

# 2. Geocode Loop
total_missing <- nrow(missing_queries)
progress_step <- 1000L

for (i in seq_len(total_missing)) {
  q <- missing_queries$geocode_query[i]
  fq <- missing_queries$fallback_query[i]
  
  result <- fetch_geocode(query = q, fallback_query = fq, api_key = api_key)
  
  geocode_cache <- rbind(
    geocode_cache,
    data.table::data.table(
      query = q, latitude = result$latitude, longitude = result$longitude,
      display_name = result$display_name, geocode_status = result$geocode_status
    ),
    fill = TRUE
  )
  
  if (i %% progress_step == 0 || i == total_missing) {
    remaining <- total_missing - i
    message(
      "Geocoding progress: done ", format(i, big.mark = ","),
      ", left ", format(remaining, big.mark = ","), "."
    )
  }

  if (i %% 50 == 0 || i == total_missing) {
    data.table::fwrite(geocode_cache, cache_file)
  }
}

# 3. Merge Results and Finalize Output
address_points <- merge(
  address_points,
  geocode_cache,
  by.x = "geocode_query",
  by.y = "query",
  all.x = TRUE
)

address_points[, match_type := data.table::fcase(
  geocode_status == "ok", "Exact Address",
  geocode_status == "ok_postcode_fallback", "Postcode Fallback",
  default = "Failed"
)]

output_cols <- c(
  intersect(c("unique_address_id", "matched_ppd_count"), names(address_points)),
  "normalized_address",
  "normalized_postcode",
  "geocode_query",
  "match_type",
  "latitude",
  "longitude",
  "display_name"
)

address_points <- unique(address_points[, ..output_cols], by = c("normalized_address", "normalized_postcode"))

data.table::fwrite(address_points, output_file)

# 4. Report Success Rates
exact_success <- sum(address_points$match_type == "Exact Address", na.rm = TRUE)
fallback_success <- sum(address_points$match_type == "Postcode Fallback", na.rm = TRUE)
total_tested <- nrow(address_points)
total_success <- exact_success + fallback_success

message("--- GEOTAGGING COMPLETE ---")
message("Mapbox Exact Matches: ", format(exact_success, big.mark = ","))
message("Mapbox Postcode Fallbacks: ", format(fallback_success, big.mark = ","))
message("Total Successfully Geocoded: ", format(total_success, big.mark = ","), " out of ", format(total_tested, big.mark = ","), " (", round((total_success/total_tested)*100, 1), "%)")
message("Output file written to: ", output_file)