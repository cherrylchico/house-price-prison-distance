# TITLE HERE

what we do


## Repository layout

- `input/`: raw inputs (EPC exports + `ppd_data.csv`), plus derived distance file `property_prison_distance.csv`
- `scripts/`: data build pipeline (download → match → materialize → geocode → pricing dataset)
- `output/`: intermediate and final outputs (match tables, caches, datasets, summaries)


## Data sources

### Price Paid Data (PPD)

- Source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
- Download strategy: yearly files, filtered to `County == Leicestershire`
- Script: `scripts/build_ppd_data.R`
- Note: downloading via `landregistry.data.gov.uk` returns only 100K rows; use the GOV.UK yearly downloads for full coverage.
  - App: https://landregistry.data.gov.uk/app/ppd/?relative_url_root=%2Fapp%2Fppd

### EPC (Domestic)

- Source: https://epc.opendatacommunities.org/downloads/domestic
- Download: “Domestic EPCs by Local Authority”
- Filter: Leicestershire LAs (see `input/` for the exact folders/files used)

## Data Preparation Pipeline

Raw inputs are expected under `input/`:
- EPC files: `input/domestic-*/certificates.csv`
- PPD: `input/ppd_data.csv`

Run steps in order:

0. **Build PPD CSV (from yearly downloads):** `scripts/build_ppd_data.R` → writes/updates `input/ppd_data.csv`
1. **Build EPC↔PPD crosswalk (no big join):** `scripts/build_epc_ppd_mapping.R`
   - Reads EPC `certificates.csv` + `input/ppd_data.csv`
   - Writes `output/epc_ppd_mapping.csv` and `output/epc_ppd_mapping_summary.txt`
2. **Materialize only matched rows:** `scripts/load_matched_epc_ppd.R`
   - Writes `output/matched_epc.csv` and `output/matched_ppd.csv`
3. **Geocode + distances:** `scripts/geocode_prison_distances.R`
   - Writes `input/property_prison_distance.csv`
   - Writes cache `output/property_geocode_cache.csv`

## EPC ↔ PPD mapping methodology

Script: `scripts/build_epc_ppd_mapping.R`

### 1) Address normalization

- Uppercase
- Replace `&` with `AND`
- Remove punctuation/special characters (keep letters and numbers)
- Collapse repeated spaces

### 2) Postcode normalization

- Uppercase
- Remove all spaces

### 3) Construct comparable address strings

- **EPC address**: use full `ADDRESS` if present; otherwise combine `ADDRESS1 + ADDRESS2 + ADDRESS3`
- **PPD address**: concatenate `saon + paon + street + locality + town`

### 4) Build match key

`match_key = normalized_postcode + "|" + normalized_address`

Rows with empty postcode/address (and therefore empty keys) are dropped before matching.

### 5) Matching passes

1. **Exact pass**: merge on exact equality of `match_key`
2. **Fuzzy pass**: for remaining unmatched records, restrict to same postcode then fuzzy-match on address similarity

### Important behavior

- The exact merge uses `allow.cartesian = TRUE`, so one-to-many / many-to-many exact-key matches are retained.
- `match_count_for_key` counts how many rows share the same key in the mapping output.

## Outputs

### Crosswalk + matched extracts

- `output/epc_ppd_mapping.csv`: match table (may contain one-to-many rows)
- `output/epc_ppd_mapping_summary.txt`: summary stats for matching
- `output/matched_epc.csv`: EPC rows corresponding to mapped matches
- `output/matched_ppd.csv`: PPD rows corresponding to mapped matches

### Distances

- `input/property_prison_distance.csv`: one row per unique matched address with prison distance
- `output/property_geocode_cache.csv`: reusable geocode cache

### Analysis datasets

- `output/hedonic_analysis_dataset.csv`: built by `Project.Rmd` (one-row-per-sale, chooses one EPC per sale)
- `output/pricing_analysis_dataset.csv`: built by `scripts/build_pricing_dataset.R` (drops failed geocodes)
- `output/pricing_analysis_dataset_summary.txt`: summary for the pricing dataset

## How to Combine All Data
- See `1_Data_Prep.Rmd`


## Match snapshot (as last recorded on 2026-03-11)

- EPC rows indexed: 481,790
- PPD rows indexed: 189,361
- Matched rows in mapping: 3,054
- Distinct matched keys: 2,194
- Distinct matched EPC certificates: 2,214
- Distinct matched PPD transactions: 3,030

| match_method                           |    N |
|----------------------------------------|-----:|
| exact_normalized_postcode_address      | 3034 |
| fuzzy_same_postcode_address_similarity |   20 |

Skipped postcode groups in fuzzy pass: 80

Among matched rows in mapping (3,054)
- with geocode: 2,670


## Distance from Prison
| deed_year | 0-2km | 2-5km | 5-10km | >10km |
|---:|---:|---:|---:|---:|
| 2011 | 6 | 13 | 0 | 118 |
| 2012 | 5 | 5 | 0 | 58 |
| 2013 | 3 | 4 | 0 | 66 |
| 2014 | 10 | 3 | 0 | 94 |
| 2015 | 5 | 7 | 0 | 75 |
| 2016 | 11 | 9 | 0 | 75 |
| 2017 | 8 | 6 | 0 | 70 |
| 2018 | 6 | 3 | 0 | 64 |
| 2019 | 3 | 4 | 0 | 38 |
| 2020 | 10 | 20 | 0 | 86 |
| 2021 | 59 | 176 | 0 | 920 |
| 2022 | 8 | 20 | 0 | 190 |
| 2023 | 15 | 10 | 0 | 107 |
| 2024 | 6 | 8 | 12 | 122 |
| 2025 | 6 | 8 | 0 | 98 |