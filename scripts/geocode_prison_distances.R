args <- commandArgs(trailingOnly = TRUE)

mapping_file <- if (length(args) >= 1) args[[1]] else file.path("output", "epc_ppd_mapping.csv")
output_file <- if (length(args) >= 2) args[[2]] else file.path("input", "property_prison_distance.csv")
cache_file <- if (length(args) >= 3) args[[3]] else file.path("output", "property_geocode_cache.csv")

if (!requireNamespace("data.table", quietly = TRUE) ||
    !requireNamespace("jsonlite", quietly = TRUE) ||
    !requireNamespace("curl", quietly = TRUE)) {
  stop("Packages 'data.table', 'jsonlite', and 'curl' are required.")
}

api_key <- Sys.getenv("GEOCODE_MAPS_API_KEY", unset = "")
if (!nzchar(api_key)) {
  stop("Set GEOCODE_MAPS_API_KEY before running this script.")
}

if (!file.exists(mapping_file)) {
  stop("Mapping file not found: ", mapping_file)
}

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cache_file), recursive = TRUE, showWarnings = FALSE)

normalize_missing <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "NODATA!", "NO DATA!")] <- NA_character_
  x
}

haversine_m <- function(lat1, lon1, lat2, lon2) {
  to_rad <- pi / 180
  lat1 <- lat1 * to_rad
  lon1 <- lon1 * to_rad
  lat2 <- lat2 * to_rad
  lon2 <- lon2 * to_rad

  dlat <- lat2 - lat1
  dlon <- lon2 - lon1

  a <- sin(dlat / 2)^2 + cos(lat1) * cos(lat2) * sin(dlon / 2)^2
  6371000 * 2 * atan2(sqrt(a), sqrt(1 - a))
}

fetch_geocode <- function(query, api_key, pause_s = 0.25, retries = 3) {
  encoded_query <- utils::URLencode(query, reserved = TRUE)
  url <- sprintf(
    "https://geocode.maps.co/search?q=%s&api_key=%s",
    encoded_query,
    api_key
  )

  last_error <- NULL

  for (attempt in seq_len(retries)) {
    response <- tryCatch(
      curl::curl_fetch_memory(url),
      error = function(e) e
    )

    if (!inherits(response, "error")) {
      status_code <- response$status_code
      body <- rawToChar(response$content)

      if (status_code >= 200 && status_code < 300) {
        parsed <- jsonlite::fromJSON(body, simplifyDataFrame = TRUE)
        if (length(parsed) == 0 || !nrow(parsed)) {
          Sys.sleep(pause_s)
          return(list(
            latitude = NA_real_,
            longitude = NA_real_,
            display_name = NA_character_,
            geocode_status = "no_result"
          ))
        }

        first_hit <- parsed[1, ]
        Sys.sleep(pause_s)
        return(list(
          latitude = as.numeric(first_hit$lat),
          longitude = as.numeric(first_hit$lon),
          display_name = as.character(first_hit$display_name),
          geocode_status = "ok"
        ))
      }

      last_error <- sprintf("HTTP %s: %s", status_code, body)
    } else {
      last_error <- conditionMessage(response)
    }

    Sys.sleep(attempt)
  }

  list(
    latitude = NA_real_,
    longitude = NA_real_,
    display_name = NA_character_,
    geocode_status = paste("error", last_error)
  )
}

mapping <- data.table::fread(mapping_file)

property_keys <- unique(mapping[, .(
  match_key,
  postcode,
  ppd_address
)])[!is.na(match_key) & !is.na(postcode) & !is.na(ppd_address)]

property_keys[, geocode_query := paste(ppd_address, postcode, "United Kingdom", sep = ", ")]

prison_query <- "HMP Fosse Way, 1 Tigers Road, Wigston, Leicestershire, LE18 4TN, United Kingdom"

if (file.exists(cache_file)) {
  geocode_cache <- data.table::fread(cache_file)
} else {
  geocode_cache <- data.table::data.table(
    query = character(),
    latitude = numeric(),
    longitude = numeric(),
    display_name = character(),
    geocode_status = character()
  )
}

needed_queries <- unique(c(prison_query, property_keys$geocode_query))
missing_queries <- setdiff(needed_queries, geocode_cache$query)

if (length(missing_queries)) {
  message("Geocoding ", length(missing_queries), " new queries.")
}

for (query in missing_queries) {
  result <- fetch_geocode(query, api_key = api_key)

  geocode_cache <- rbind(
    geocode_cache,
    data.table::data.table(
      query = query,
      latitude = result$latitude,
      longitude = result$longitude,
      display_name = result$display_name,
      geocode_status = result$geocode_status
    ),
    fill = TRUE
  )

  data.table::fwrite(geocode_cache, cache_file)
}

prison_location <- geocode_cache[query == prison_query][1]

if (!nrow(prison_location) || is.na(prison_location$latitude) || is.na(prison_location$longitude)) {
  stop("Failed to geocode HMP Fosse Way. Check the API key or query.")
}

property_points <- merge(
  property_keys,
  geocode_cache,
  by.x = "geocode_query",
  by.y = "query",
  all.x = TRUE
)

property_points[, distance_to_prison_m := haversine_m(
  latitude,
  longitude,
  prison_location$latitude,
  prison_location$longitude
)]

distance_lookup <- merge(
  unique(mapping[, .(ppd_id, lmk_key, match_key)]),
  property_points[, .(
    match_key,
    geocode_query,
    latitude,
    longitude,
    display_name,
    geocode_status,
    distance_to_prison_m
  )],
  by = "match_key",
  all.x = TRUE
)

data.table::setnames(distance_lookup, c("ppd_id", "lmk_key"), c("unique_id", "LMK_KEY"))
data.table::fwrite(distance_lookup, output_file)

message("Distance file written to: ", output_file)
message("Cache file written to: ", cache_file)
message("Prison geocoded to: ", prison_location$display_name)
