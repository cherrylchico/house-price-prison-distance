args <- commandArgs(trailingOnly = TRUE)

mapping_file <- if (length(args) >= 1) args[[1]] else file.path("output", "epc_ppd_mapping.csv")
output_file <- if (length(args) >= 2) args[[2]] else file.path("output", "address_geocodes.csv")
cache_file <- if (length(args) >= 3) args[[3]] else file.path("output", "address_geocode_cache.csv")
postcode_filter_file <- if (length(args) >= 4) args[[4]] else file.path("output", "postcode_prison_distance_filter.csv")

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("jsonlite", quietly = TRUE) ||
    !requireNamespace("curl", quietly = TRUE)) {
  stop("Packages 'data.table', 'jsonlite', and 'curl' are required.")
}

# Your Mapbox Access Token
api_key <- "pk.eyJ1IjoiZG1lYXJzMTIzIiwiYSI6ImNtbXF1a2NoajEwdGQycnF4MnJmb3M5dzcifQ.YdbAmwfSh1D1bJsqGaVqyw"

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
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

# 1. Load mappings
mapping <- data.table::fread(mapping_file)
property_keys <- unique(mapping[!is.na(match_key) & !is.na(postcode), .(match_key, postcode)])

# 2. Apply Postcode Distance Filter (ONLY Geocode what we need)
if (file.exists(postcode_filter_file)) {
  postcode_filter_raw <- data.table::fread(postcode_filter_file)
  
  if (!"keep_for_full_geocode" %in% names(postcode_filter_raw)) {
    stop("Postcode filter file is missing the required column: 'keep_for_full_geocode'")
  }
  
  postcode_filter <- unique(postcode_filter_raw[, .(match_key, postcode, keep_for_full_geocode)])
  
  property_keys <- merge(
    property_keys,
    postcode_filter,
    by = c("match_key", "postcode"),
    all.x = TRUE
  )
  
  total_property_keys <- nrow(property_keys)
  property_keys <- property_keys[keep_for_full_geocode == TRUE]
  
  message(
    "Postcode prefilter retained ", format(nrow(property_keys), big.mark = ","),
    " of ", format(total_property_keys, big.mark = ","),
    " matched property keys for exact address geocoding."
  )
} else {
  message("WARNING: Postcode filter file not found. Proceeding to geocode ALL addresses.")
}

# 3. Prepare for Geocoding
address_points <- data.table::copy(property_keys)

# Split the match_key into Postcode and Address (format: POSTCODE|ADDRESS)
address_points[, parsed_postcode := sub("\\|.*", "", match_key)]
address_points[, parsed_street := sub(".*\\|", "", match_key)]

# Formulate queries
address_points[, geocode_query := paste(parsed_street, parsed_postcode, sep = ", ")]
address_points[, fallback_query := parsed_postcode]

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

# 4. Geocode Loop
for (i in seq_len(nrow(missing_queries))) {
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
  
  if (i %% 50 == 0 || i == nrow(missing_queries)) {
    data.table::fwrite(geocode_cache, cache_file)
  }
}

# 5. Merge Results and Finalize Output
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

address_points <- unique(address_points[, .(
  match_key,
  postcode,
  geocode_query,
  match_type,
  latitude,
  longitude,
  display_name
)], by = "match_key")

data.table::fwrite(address_points, output_file)

# 6. Report Success Rates
exact_success <- sum(address_points$match_type == "Exact Address", na.rm = TRUE)
fallback_success <- sum(address_points$match_type == "Postcode Fallback", na.rm = TRUE)
total_tested <- nrow(address_points)
total_success <- exact_success + fallback_success

message("--- GEOTAGGING COMPLETE ---")
message("Mapbox Exact Matches: ", format(exact_success, big.mark = ","))
message("Mapbox Postcode Fallbacks: ", format(fallback_success, big.mark = ","))
message("Total Successfully Geocoded: ", format(total_success, big.mark = ","), " out of ", format(total_tested, big.mark = ","), " (", round((total_success/total_tested)*100, 1), "%)")
message("Output file written to: ", output_file)