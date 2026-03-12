# Geospatial Final Project

This repository builds a matched EPC and Price Paid Data dataset for Leicestershire-focused analysis, enriches matched rows with LSOA codes, filters postcodes using an HMP Fosse Way bounding-box workflow, and then geocodes retained postcodes to estimate prison distance efficiently.

## Repository layout

- `input/`: raw inputs and derived final distance file
- `output/`: intermediate outputs, caches, filtered lookups, summaries, and matched extracts
- `scripts/`: R pipeline scripts used to build, filter, enrich, and geocode the data
- `1_Data_Prep.Rmd`: notebook-style workflow for downstream preparation/combination

## Data sources

### Price Paid Data (PPD)

- Source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
- Download strategy: yearly CSV files listed in `input/ppd_link.txt`
- Current county filter supports one or more values and is case-insensitive
- Current workflow filters for both `LEICESTER` and `LEICESTERSHIRE`

### EPC (Domestic)

- Source: https://epc.opendatacommunities.org/downloads/domestic
- Input pattern: `input/domestic-*/certificates.csv`
- Current set is the local-authority exports used for Leicester/Leicestershire analysis

### Postcode to LSOA lookup

- Input file: `input/PCD_OA21_LSOA21_MSOA21_LAD_AUG24_UK_LU.csv`
- Used to attach `lsoa21cd` to matched EPC/PPD rows and to build a LAD-filtered postcode lookup

### LSOA boundaries

- Input file: `input/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW.geojson`
- Used to select LSOAs intersecting the HMP Fosse Way square bounding box

## Environment

R packages currently required by the pipeline:

- `data.table`
- `sf`
- `jsonlite`
- `curl`

Optional Python environment created locally:

```bash
source .venv/bin/activate
```

## Current pipeline

Run the scripts in this order.

### 1. Build filtered PPD file

Script: `scripts/build_ppd_data.R`

Purpose:

- Reads yearly download URLs from `input/ppd_link.txt`
- Applies standard PPD column names
- Filters rows by one or more county values
- Writes `input/ppd_data.csv`

Run:

```bash
Rscript scripts/build_ppd_data.R input/ppd_link.txt input/ppd_data.csv LEICESTER LEICESTERSHIRE
```

Notes:

- You can also pass a comma-separated county list as a single argument.
- The filter is case-insensitive.

### 2. Build EPC ↔ PPD mapping

Script: `scripts/build_epc_ppd_mapping.R`

Purpose:

- Reads EPC certificates and filtered PPD data
- Normalizes postcode and address fields
- Produces exact and fuzzy mapping passes
- Writes `output/epc_ppd_mapping.csv` and `output/epc_ppd_mapping_summary.txt`

Run:

```bash
Rscript scripts/build_epc_ppd_mapping.R
```

### 3. Materialize matched EPC and PPD extracts with LSOA enrichment

Script: `scripts/load_matched_epc_ppd.R`

Purpose:

- Reads the mapping output
- Excludes PPD districts:
   - `NORTH WEST LEICESTERSHIRE`
   - `MELTON`
- Builds a postcode lookup filtered to these LAD codes:
   - `E06000016`
   - `E07000130`
   - `E07000131`
   - `E07000135`
   - `E07000129`
   - `E07000132`
   - `E07000133`
   - `E07000134`
- Joins `lsoa21cd` onto both matched outputs using postcode with spaces removed
- Writes:
   - `output/matched_epc.csv`
   - `output/matched_ppd.csv`
   - `output/postcode_lsoa_lookup_target_lads.csv`

Run:

```bash
Rscript scripts/load_matched_epc_ppd.R
```

### 4. Filter postcodes using an HMP Fosse Way bounding box and intersecting LSOAs

Script: `scripts/filter_postcode_prison_distance.R`

Purpose:

- Uses the fixed point for HMP Fosse Way:
   - latitude: `52.584126`
   - longitude: `-1.145212`
- Builds an `11 km` square bounding box around that point
- Selects all LSOA polygons intersecting that box
- Uses the postcode-to-LSOA lookup to flag matched postcode rows retained for geocoding
- Writes:
   - `output/postcode_prison_distance_filter.csv`
   - `output/postcode_prison_distance_candidates.csv`
   - `output/fosse_way_lsoa_bbox_candidates.geojson`
   - `output/postcode_prison_distance_filter_summary.txt`

Run:

```bash
Rscript scripts/filter_postcode_prison_distance.R
```

### 5. Geocode retained postcodes and compute prison distances

Script: `scripts/geocode_prison_distances.R`

Purpose:

- Reads `output/postcode_prison_distance_filter.csv`
- Keeps only rows where `keep_for_full_geocode == TRUE`
- Geocodes one point per postcode, not one point per address
- Uses the fixed HMP Fosse Way point above for the distance calculation
- Joins postcode-level coordinates/distances back onto matched property rows
- Writes:
   - `input/property_prison_distance.csv`
   - `output/property_geocode_cache.csv`

Run:

```bash
export GEOCODE_MAPS_API_KEY='your_api_key_here'
Rscript scripts/geocode_prison_distances.R
```

## Mapping methodology summary

The EPC/PPD mapping currently uses four passes:

1. Exact match on normalized postcode + normalized address
2. Fuzzy same-postcode address similarity
3. Exact same-postcode match after stripping EPC locality terms derived from `ADDRESS2 + ADDRESS3`
4. Fuzzy same-postcode match after stripping EPC locality terms

Address normalization includes:

- uppercase conversion
- replacing `&` with `AND`
- removing non-alphanumeric separators
- collapsing repeated whitespace

Postcode normalization includes:

- uppercase conversion
- removal of all spaces

## Current outputs and counts

### Mapping snapshot

From `output/epc_ppd_mapping_summary.txt`:

- EPC rows indexed: 481,790
- PPD rows indexed: 241,110
- Matched rows in mapping: 294,886
- Distinct matched keys: 156,801
- Distinct matched EPC certificates: 212,759
- Distinct matched PPD transactions: 209,307
- Skipped postcode groups in fuzzy passes: 72

Match counts by method:

| match_method | N |
|---|---:|
| exact_normalized_postcode_address | 146,733 |
| exact_same_postcode_stripped_epc_locality | 147,605 |
| fuzzy_same_postcode_address_similarity | 182 |
| fuzzy_same_postcode_stripped_epc_locality | 366 |

### Matched extract snapshot

- `output/matched_epc.csv`: 175,031 rows
- `output/matched_ppd.csv`: 169,706 rows
- `matched_epc.csv` rows with non-missing `lsoa21cd`: 174,865
- `matched_ppd.csv` rows with non-missing `lsoa21cd`: 169,539

### Bounding-box postcode filter snapshot

From `output/postcode_prison_distance_filter_summary.txt`:

- Bounding-box half-width/height: 11 km
- Unique property keys reviewed: 156,801
- Unique postcodes reviewed: 16,915
- Distinct selected LSOAs intersecting the bbox: 364
- Postcodes with postcode-to-LSOA match: 156,519
- Unique postcodes retained via LSOA bbox filter: 9,292
- Retained via LSOA bbox filter: 81,170
- Excluded by LSOA bbox filter: 75,631

### Geocoding status

- The geocoding script now works at postcode level for efficiency.
- With the current filter, the intended workload is 9,292 postcode queries rather than 81,170 address queries.
- `input/property_prison_distance.csv` should be treated as in-progress or stale until the current geocoding run finishes and rewrites it.

## Main outputs

- `input/ppd_data.csv`: filtered PPD source table
- `output/epc_ppd_mapping.csv`: EPC/PPD mapping table
- `output/epc_ppd_mapping_summary.txt`: mapping summary
- `output/matched_epc.csv`: matched EPC rows with `lsoa21cd`
- `output/matched_ppd.csv`: matched PPD rows with `lsoa21cd`
- `output/postcode_lsoa_lookup_target_lads.csv`: reduced postcode-to-LSOA/LAD lookup
- `output/postcode_prison_distance_filter.csv`: full postcode filter table with `keep_for_full_geocode`
- `output/postcode_prison_distance_candidates.csv`: retained postcode subset
- `output/fosse_way_lsoa_bbox_candidates.geojson`: intersecting LSOA polygons
- `output/postcode_prison_distance_filter_summary.txt`: bbox filter summary
- `output/property_geocode_cache.csv`: postcode geocode cache
- `input/property_prison_distance.csv`: joined prison distance output

## Downstream combination

For the downstream data-prep combination step, see `1_Data_Prep.Rmd`.
