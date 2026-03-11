args <- commandArgs(trailingOnly = TRUE)

input_dir <- if (length(args) >= 1) args[[1]] else "input"
output_dir <- if (length(args) >= 2) args[[2]] else "output"

if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required. Install it with install.packages('data.table').")
}

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

normalize_text <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(trimws(x))
  x <- gsub("&", " AND ", x, fixed = TRUE)
  x <- gsub("[^A-Z0-9]+", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

normalize_postcode <- function(x) {
  x <- ifelse(is.na(x), "", x)
  x <- toupper(gsub("\\s+", "", x))
  trimws(x)
}

epc_address_string <- function(dt) {
  combined <- paste(dt$ADDRESS1, dt$ADDRESS2, dt$ADDRESS3)
  combined <- gsub("\\s+", " ", combined)
  combined <- trimws(combined)
  has_full_address <- !is.na(dt$ADDRESS) & nzchar(trimws(dt$ADDRESS))
  ifelse(has_full_address, trimws(dt$ADDRESS), combined)
}

build_match_key <- function(postcode, address) {
  paste(normalize_postcode(postcode), normalize_text(address), sep = "|")
}

remove_component_words <- function(address, components) {
  address_norm <- normalize_text(address)
  components_norm <- normalize_text(components)

  out <- mapply(
    function(address_value, component_value) {
      if (!nzchar(address_value)) {
        return(address_value)
      }

      if (!nzchar(component_value)) {
        return(address_value)
      }

      component_words <- unique(strsplit(component_value, " ", fixed = TRUE)[[1]])
      component_words <- component_words[nzchar(component_words)]

      if (!length(component_words)) {
        return(address_value)
      }

      address_words <- strsplit(address_value, " ", fixed = TRUE)[[1]]
      kept_words <- address_words[!address_words %in% component_words]

      trimws(paste(kept_words, collapse = " "))
    },
    address_norm,
    components_norm,
    USE.NAMES = FALSE
  )

  out
}

extract_house_token <- function(address_key) {
  match_position <- regexpr("\\b[0-9]+[A-Z]?\\b", address_key, perl = TRUE)
  out <- rep(NA_character_, length(address_key))
  matched_index <- match_position != -1L
  out[matched_index] <- regmatches(address_key, match_position)
  out
}

address_similarity <- function(x, y) {
  distance <- mapply(
    function(left, right) utils::adist(left, right)[1, 1],
    x,
    y,
    USE.NAMES = FALSE
  )
  denominator <- pmax(nchar(x), nchar(y), 1L)
  1 - as.numeric(distance) / denominator
}

empty_mapping_table <- function() {
  data.table::data.table(
    match_method = character(),
    match_count_for_key = integer(),
    match_score = numeric(),
    match_key = character(),
    postcode_key = character(),
    address_key = character(),
    epc_address_key = character(),
    ppd_address_key = character(),
    epc_source = character(),
    epc_file = character(),
    lmk_key = character(),
    uprn = character(),
    inspection_date = character(),
    lodgement_date = character(),
    postcode = character(),
    epc_address = character(),
    ppd_id = character(),
    deed_date = character(),
    price_paid = numeric(),
    ppd_address = character(),
    district = character(),
    county = character()
  )
}

empty_fuzzy_result <- function() {
  list(matches = empty_mapping_table(), skipped_postcodes = character())
}

build_fuzzy_mapping <- function(epc_dt, ppd_dt, matched_dt,
                                epc_key_col = "epc_match_key",
                                epc_address_col = "epc_address_key",
                                ppd_key_col = "ppd_match_key",
                                ppd_address_col = "ppd_address_key",
                                match_method = "fuzzy_same_postcode_address_similarity",
                                similarity_threshold = 0.9,
                                max_pairs_per_postcode = 5000L) {
  matched_epc_ids <- unique(matched_dt$lmk_key)
  matched_ppd_ids <- unique(matched_dt$ppd_id)

  epc_unmatched <- epc_dt[!lmk_key %chin% matched_epc_ids]
  ppd_unmatched <- ppd_dt[!ppd_id %chin% matched_ppd_ids]

  if (!nrow(epc_unmatched) || !nrow(ppd_unmatched)) {
    return(empty_fuzzy_result())
  }

  common_postcodes <- intersect(unique(epc_unmatched$postcode_key), unique(ppd_unmatched$postcode_key))

  if (!length(common_postcodes)) {
    return(empty_fuzzy_result())
  }

  fuzzy_matches <- vector("list", length(common_postcodes))
  skipped_postcodes <- character()
  match_index <- 0L

  for (postcode_value in common_postcodes) {
    epc_group <- epc_unmatched[postcode_key == postcode_value]
    ppd_group <- ppd_unmatched[postcode_key == postcode_value]

    if (!nrow(epc_group) || !nrow(ppd_group)) {
      next
    }

    pair_count <- nrow(epc_group) * nrow(ppd_group)
    if (pair_count > max_pairs_per_postcode) {
      skipped_postcodes <- c(skipped_postcodes, postcode_value)
      next
    }

    candidates <- merge(
      epc_group,
      ppd_group,
      by = "postcode_key",
      allow.cartesian = TRUE,
      suffixes = c("_epc", "_ppd")
    )

    candidates <- candidates[
      is.na(house_token_epc) | is.na(house_token_ppd) | house_token_epc == house_token_ppd
    ]

    if (!nrow(candidates)) {
      next
    }

    candidates[, match_score := address_similarity(get(epc_address_col), get(ppd_address_col))]
    candidates[, address_length_gap := abs(nchar(get(epc_address_col)) - nchar(get(ppd_address_col)))]
    candidates <- candidates[match_score >= similarity_threshold]

    if (!nrow(candidates)) {
      next
    }

    data.table::setorder(candidates, -match_score, address_length_gap, lmk_key, ppd_id)
    candidates[, epc_rank := seq_len(.N), by = lmk_key]
    candidates[, ppd_rank := seq_len(.N), by = ppd_id]

    mutual_best <- candidates[epc_rank == 1L & ppd_rank == 1L,
      .(
        match_method = match_method,
        match_count_for_key = NA_integer_,
        match_score,
        match_key = get(ppd_key_col),
        postcode_key,
        address_key = get(ppd_address_col),
        epc_address_key = get(epc_address_col),
        ppd_address_key = get(ppd_address_col),
        epc_source,
        epc_file,
        lmk_key,
        uprn,
        inspection_date,
        lodgement_date,
        postcode = postcode_ppd,
        epc_address,
        ppd_id,
        deed_date,
        price_paid,
        ppd_address,
        district,
        county
      )]

    if (!nrow(mutual_best)) {
      next
    }

    match_index <- match_index + 1L
    fuzzy_matches[[match_index]] <- mutual_best
  }

  if (!match_index) {
    return(list(matches = empty_mapping_table(), skipped_postcodes = skipped_postcodes))
  }

  list(
    matches = data.table::rbindlist(fuzzy_matches[seq_len(match_index)], use.names = TRUE, fill = TRUE),
    skipped_postcodes = skipped_postcodes
  )
}

build_exact_mapping <- function(epc_dt, ppd_dt, matched_dt,
                                epc_key_col = "epc_match_key",
                                epc_address_col = "epc_address_key",
                                ppd_key_col = "ppd_match_key",
                                ppd_address_col = "ppd_address_key",
                                match_method = "exact_normalized_postcode_address") {
  matched_epc_ids <- unique(matched_dt$lmk_key)
  matched_ppd_ids <- unique(matched_dt$ppd_id)

  epc_unmatched <- epc_dt[!lmk_key %chin% matched_epc_ids]
  ppd_unmatched <- ppd_dt[!ppd_id %chin% matched_ppd_ids]

  if (!nrow(epc_unmatched) || !nrow(ppd_unmatched)) {
    return(empty_mapping_table())
  }

  epc_exact <- data.table::copy(epc_unmatched)
  ppd_exact <- data.table::copy(ppd_unmatched)

  epc_exact[, exact_join_key := get(epc_key_col)]
  ppd_exact[, exact_join_key := get(ppd_key_col)]

  merged_dt <- merge(
    epc_exact,
    ppd_exact,
    by = c("exact_join_key", "postcode_key"),
    allow.cartesian = TRUE,
    suffixes = c("_epc", "_ppd")
  )

  if (!nrow(merged_dt)) {
    return(empty_mapping_table())
  }

  merged_dt[, .(
    match_method = match_method,
    match_count_for_key = NA_integer_,
    match_score = 1,
    match_key = exact_join_key,
    postcode_key,
    address_key = get(ppd_address_col),
    epc_address_key = get(epc_address_col),
    ppd_address_key = get(ppd_address_col),
    epc_source,
    epc_file,
    lmk_key,
    uprn,
    inspection_date,
    lodgement_date,
    postcode = postcode_ppd,
    epc_address,
    ppd_id,
    deed_date,
    price_paid,
    ppd_address,
    district,
    county
  )]
}

epc_files <- list.files(
  input_dir,
  pattern = "^certificates\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

epc_files <- epc_files[grepl("/domestic-", epc_files)]

if (!length(epc_files)) {
  stop("No EPC certificates.csv files were found under the input directory.")
}

message("Reading EPC certificates from ", length(epc_files), " folders.")

epc_index <- data.table::rbindlist(
  lapply(epc_files, function(file_path) {
    dt <- data.table::fread(
      file_path,
      select = c(
        "LMK_KEY", "ADDRESS1", "ADDRESS2", "ADDRESS3", "ADDRESS",
        "POSTCODE", "UPRN", "INSPECTION_DATE", "LODGEMENT_DATE"
      ),
      na.strings = c("", "NA", "NODATA!", "NO DATA!")
    )

    dt[, epc_source := basename(dirname(file_path))]
    dt[, epc_file := file_path]
    dt[, epc_address := epc_address_string(.SD)]
    dt[, postcode_key := normalize_postcode(POSTCODE)]
    dt[, address_key := normalize_text(epc_address)]
    dt[, match_key := build_match_key(POSTCODE, epc_address)]
    dt[, epc_address_key := address_key]
    dt[, epc_match_key := match_key]
    dt[, locality_key := normalize_text(paste(ADDRESS2, ADDRESS3))]
    dt[, epc_address_key_stripped := remove_component_words(epc_address, paste(ADDRESS2, ADDRESS3))]
    dt[, epc_match_key_stripped := build_match_key(POSTCODE, epc_address_key_stripped)]
    dt[, house_token := extract_house_token(epc_address_key)]
    dt[nzchar(postcode_key) & nzchar(address_key),
      .(
        epc_source,
        epc_file,
        lmk_key = LMK_KEY,
        uprn = UPRN,
        inspection_date = INSPECTION_DATE,
        lodgement_date = LODGEMENT_DATE,
        postcode = POSTCODE,
        epc_address,
        postcode_key,
        address_key,
        match_key,
        epc_address_key,
        epc_match_key,
        locality_key,
        epc_address_key_stripped,
        epc_match_key_stripped,
        house_token
      )]
  }),
  use.names = TRUE,
  fill = TRUE
)

ppd_file <- file.path(input_dir, "ppd_data.csv")

if (!file.exists(ppd_file)) {
  stop("The file 'ppd_data.csv' was not found under the input directory.")
}

message("Reading PPD data.")

ppd_index <- data.table::fread(
  ppd_file,
  select = c(
    "unique_id", "price_paid", "deed_date", "postcode",
    "saon", "paon", "street", "locality", "town", "district", "county"
  ),
  na.strings = c("", "NA")
)

ppd_index[, ppd_address := trimws(gsub(
  "\\s+",
  " ",
  paste(saon, paon, street)
))]
ppd_index[, postcode_key := normalize_postcode(postcode)]
ppd_index[, address_key := normalize_text(ppd_address)]
ppd_index[, match_key := build_match_key(postcode, ppd_address)]
ppd_index[, ppd_address_key := address_key]
ppd_index[, ppd_match_key := match_key]
ppd_index[, house_token := extract_house_token(ppd_address_key)]
ppd_index <- ppd_index[nzchar(postcode_key) & nzchar(address_key),
  .(
    ppd_id = unique_id,
    deed_date,
    price_paid,
    postcode,
    ppd_address,
    district,
    county,
    postcode_key,
    address_key,
    match_key,
    ppd_address_key,
    ppd_match_key,
    house_token
  )]

message("Joining EPC and PPD on exact normalized postcode + address.")

mapping_exact_raw <- merge(
  epc_index,
  ppd_index,
  by = c("match_key", "postcode_key", "address_key"),
  allow.cartesian = TRUE
)

mapping_exact <- mapping_exact_raw[, .(
  match_method = "exact_normalized_postcode_address",
  match_count_for_key = NA_integer_,
  match_score = 1,
  match_key = ppd_match_key,
  postcode_key,
  address_key = ppd_address_key,
  epc_address_key,
  ppd_address_key,
  epc_source,
  epc_file,
  lmk_key,
  uprn,
  inspection_date,
  lodgement_date,
  postcode = postcode.y,
  epc_address,
  ppd_id,
  deed_date,
  price_paid,
  ppd_address,
  district,
  county
)]

message("Running fuzzy same-postcode matching on unmatched rows.")

fuzzy_result <- build_fuzzy_mapping(epc_index, ppd_index, mapping_exact)
mapping_exact_stripped <- build_exact_mapping(
  epc_index,
  ppd_index,
  data.table::rbindlist(list(mapping_exact, fuzzy_result$matches), use.names = TRUE, fill = TRUE),
  epc_key_col = "epc_match_key_stripped",
  epc_address_col = "epc_address_key_stripped",
  ppd_key_col = "ppd_match_key",
  ppd_address_col = "ppd_address_key",
  match_method = "exact_same_postcode_stripped_epc_locality"
)

message("Running fuzzy same-postcode matching on unmatched rows after stripping EPC locality terms.")

fuzzy_result_stripped <- build_fuzzy_mapping(
  epc_index,
  ppd_index,
  data.table::rbindlist(
    list(mapping_exact, fuzzy_result$matches, mapping_exact_stripped),
    use.names = TRUE,
    fill = TRUE
  ),
  epc_key_col = "epc_match_key_stripped",
  epc_address_col = "epc_address_key_stripped",
  ppd_key_col = "ppd_match_key",
  ppd_address_col = "ppd_address_key",
  match_method = "fuzzy_same_postcode_stripped_epc_locality"
)

mapping <- data.table::rbindlist(
  list(
    mapping_exact,
    fuzzy_result$matches,
    mapping_exact_stripped,
    fuzzy_result_stripped$matches
  ),
  use.names = TRUE,
  fill = TRUE
)
mapping[, match_count_for_key := .N, by = match_key]

data.table::setcolorder(
  mapping,
  c(
    "match_method", "match_count_for_key", "match_score", "match_key",
    "postcode_key", "address_key", "epc_address_key", "ppd_address_key",
    "epc_source", "epc_file", "lmk_key", "uprn", "inspection_date",
    "lodgement_date", "postcode", "epc_address",
    "ppd_id", "deed_date", "price_paid", "ppd_address", "district", "county"
  )
)

mapping_file <- file.path(output_dir, "epc_ppd_mapping.csv")
data.table::fwrite(mapping, mapping_file)

summary_file <- file.path(output_dir, "epc_ppd_mapping_summary.txt")
method_counts <- mapping[, .N, by = match_method][order(match_method)]
summary_lines <- c(
  paste("EPC rows indexed:", format(nrow(epc_index), big.mark = ",")),
  paste("PPD rows indexed:", format(nrow(ppd_index), big.mark = ",")),
  paste("Matched rows in mapping:", format(nrow(mapping), big.mark = ",")),
  paste("Distinct matched keys:", format(data.table::uniqueN(mapping$match_key), big.mark = ",")),
  paste("Distinct matched EPC certificates:", format(data.table::uniqueN(mapping$lmk_key), big.mark = ",")),
  paste("Distinct matched PPD transactions:", format(data.table::uniqueN(mapping$ppd_id), big.mark = ",")),
  paste("Match counts by method:"),
  paste(capture.output(print(method_counts)), collapse = "\n"),
  paste(
    "Skipped postcode groups in fuzzy passes:",
    length(unique(c(fuzzy_result$skipped_postcodes, fuzzy_result_stripped$skipped_postcodes)))
  )
)
writeLines(summary_lines, summary_file)

message("Mapping written to: ", mapping_file)
message("Summary written to: ", summary_file)
