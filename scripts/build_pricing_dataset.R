args <- commandArgs(trailingOnly = TRUE)

mapping_file <- if (length(args) >= 1) args[[1]] else file.path("output", "epc_ppd_mapping.csv")
matched_epc_file <- if (length(args) >= 2) args[[2]] else file.path("output", "matched_epc.csv")
matched_ppd_file <- if (length(args) >= 3) args[[3]] else file.path("output", "matched_ppd.csv")
distance_file <- if (length(args) >= 4) args[[4]] else file.path("input", "property_prison_distance.csv")
output_file <- if (length(args) >= 5) args[[5]] else file.path("output", "pricing_analysis_dataset.csv")
prison_open_year <- if (length(args) >= 6) as.integer(args[[6]]) else 2017L

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required.")
}

normalize_missing <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "NODATA!", "NO DATA!")] <- NA_character_
  x
}

construction_band_to_year <- function(x) {
  bands <- toupper(normalize_missing(x))
  out <- rep(NA_real_, length(bands))

  mappings <- list(
    "ENGLAND AND WALES: BEFORE 1900" = 1890,
    "ENGLAND AND WALES: 1900-1929" = 1915,
    "ENGLAND AND WALES: 1930-1949" = 1940,
    "ENGLAND AND WALES: 1950-1966" = 1958,
    "ENGLAND AND WALES: 1967-1975" = 1971,
    "ENGLAND AND WALES: 1976-1982" = 1979,
    "ENGLAND AND WALES: 1983-1990" = 1986.5,
    "ENGLAND AND WALES: 1991-1995" = 1993,
    "ENGLAND AND WALES: 1996-2002" = 1999,
    "ENGLAND AND WALES: 2003-2006" = 2004.5,
    "ENGLAND AND WALES: 2007 ONWARDS" = 2010,
    "ENGLAND AND WALES: 2007-2011" = 2009,
    "ENGLAND AND WALES: 2012 ONWARDS" = 2014
  )

  for (band_name in names(mappings)) {
    out[bands == band_name] <- mappings[[band_name]]
  }

  out
}

postcode_district_from_postcode <- function(postcode) {
  postcode <- toupper(gsub("\\s+", "", normalize_missing(postcode)))
  district <- sub("^([A-Z]{1,2}[0-9][0-9A-Z]?).*$", "\\1", postcode)
  district[!nzchar(postcode)] <- NA_character_
  district
}

assign_ring <- function(distance_m) {
  data.table::fcase(
    is.na(distance_m), NA_character_,
    distance_m <= 200, "0-200m",
    distance_m <= 500, "201-500m",
    distance_m <= 2000, "501-2000m",
    default = "Control"
  )
}

pick_best_epc_per_sale <- function(mapping_dt) {
  dt <- data.table::copy(mapping_dt)

  dt[, inspection_date := as.Date(inspection_date)]
  dt[, lodgement_date := as.Date(lodgement_date)]
  dt[, deed_date := as.Date(deed_date)]
  dt[, epc_event_date := as.Date(ifelse(!is.na(lodgement_date), lodgement_date, inspection_date))]
  dt[, days_diff := as.integer(deed_date - epc_event_date)]
  dt[, abs_days_diff := abs(days_diff)]
  dt[, epc_before_sale := !is.na(days_diff) & days_diff >= 0]

  data.table::setorder(dt, unique_id, -epc_before_sale, abs_days_diff, -epc_event_date)
  dt[, .SD[1], by = unique_id]
}

if (!file.exists(mapping_file) || !file.exists(matched_epc_file) ||
    !file.exists(matched_ppd_file) || !file.exists(distance_file)) {
  stop("One or more required input files are missing.")
}

dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)

mapping <- data.table::fread(mapping_file)
matched_epc <- data.table::fread(matched_epc_file)
matched_ppd <- data.table::fread(matched_ppd_file)
distance_dt <- data.table::fread(distance_file)

data.table::setnames(mapping, c("ppd_id", "lmk_key"), c("unique_id", "LMK_KEY"), skip_absent = TRUE)

best_match <- pick_best_epc_per_sale(mapping)

distance_keep <- unique(distance_dt[
  !is.na(distance_to_prison_m) & geocode_status == "ok",
  .(
    unique_id,
    LMK_KEY,
    distance_to_prison_m,
    geocode_query,
    latitude,
    longitude,
    geocode_status,
    geocoded_address = display_name
  )
], by = "unique_id")

analysis_dt <- merge(best_match, distance_keep, by = c("unique_id", "LMK_KEY"), all = FALSE)
analysis_dt <- merge(analysis_dt, matched_ppd, by = "unique_id", all.x = TRUE, suffixes = c("_map", "_ppd"))
analysis_dt <- merge(analysis_dt, matched_epc, by = "LMK_KEY", all.x = TRUE, suffixes = c("", "_epc"))

analysis_dt[, sale_price := as.numeric(price_paid_ppd)]
analysis_dt[, deed_date := as.Date(deed_date_ppd)]
analysis_dt[, sale_year := as.integer(format(deed_date, "%Y"))]
analysis_dt[, log_price := log(sale_price)]
analysis_dt[, rel_year := sale_year - prison_open_year]
analysis_dt[, ring := assign_ring(distance_to_prison_m)]

analysis_dt[, total_floor_area := suppressWarnings(as.numeric(TOTAL_FLOOR_AREA))]
analysis_dt[, habitable_rooms := suppressWarnings(as.numeric(NUMBER_HABITABLE_ROOMS))]
analysis_dt[, built_year_mid := construction_band_to_year(CONSTRUCTION_AGE_BAND)]
analysis_dt[, age_at_sale := ifelse(!is.na(built_year_mid), sale_year - built_year_mid, NA_real_)]
analysis_dt[, postcode_district := postcode_district_from_postcode(POSTCODE)]

analysis_dt[, bathrooms := NA_real_]
analysis_dt[, lot_size := NA_real_]

analysis_dt <- analysis_dt[
  !is.na(sale_price) &
  sale_price > 0 &
  !is.na(distance_to_prison_m) &
  !is.na(ring)
]

analysis_dt <- unique(analysis_dt, by = "unique_id")

final_dt <- analysis_dt[, .(
  unique_id,
  LMK_KEY,
  uprn,
  sale_price,
  log_price,
  deed_date,
  sale_year,
  rel_year,
  distance_to_prison_m,
  ring,
  postcode = POSTCODE,
  postcode_district,
  epc_address,
  ppd_address,
  property_type_ppd = property_type,
  transaction_category,
  PROPERTY_TYPE,
  BUILT_FORM,
  total_floor_area,
  bedrooms = NA_real_,
  habitable_rooms,
  bathrooms,
  built_year_mid,
  age_at_sale,
  lot_size,
  CONSTRUCTION_AGE_BAND,
  CURRENT_ENERGY_RATING,
  CURRENT_ENERGY_EFFICIENCY,
  POTENTIAL_ENERGY_RATING,
  POTENTIAL_ENERGY_EFFICIENCY,
  inspection_date,
  lodgement_date,
  epc_source,
  geocode_query,
  geocoded_address,
  latitude,
  longitude,
  geocode_status
)]

data.table::fwrite(final_dt, output_file)

summary_file <- file.path(dirname(output_file), "pricing_analysis_dataset_summary.txt")
summary_lines <- c(
  paste("Rows in final dataset:", format(nrow(final_dt), big.mark = ",")),
  paste("Distinct sales:", format(data.table::uniqueN(final_dt$unique_id), big.mark = ",")),
  paste("Sale year range:", paste(range(final_dt$sale_year, na.rm = TRUE), collapse = " to ")),
  paste("Ring counts:"),
  paste(capture.output(print(final_dt[, .N, by = ring][order(ring)])), collapse = "\n"),
  paste("Missing total_floor_area:", sum(is.na(final_dt$total_floor_area))),
  paste("Missing habitable_rooms:", sum(is.na(final_dt$habitable_rooms))),
  paste("Missing age_at_sale:", sum(is.na(final_dt$age_at_sale)))
)
writeLines(summary_lines, summary_file)

message("Pricing dataset written to: ", output_file)
message("Summary written to: ", summary_file)
