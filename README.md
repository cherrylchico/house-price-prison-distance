# Housing Price Analysis Data Pipeline for a Defined Study Area in UK

This repository builds a matched EPC and Price Paid Data (PPD) dataset for a defined study area. It applies target-LSOA filtering to matched PPD rows via postcode-to-LSOA lookup, geocodes retained addresses, and computes distance to a target location.

The current target location is Walleys Quarry Landfill (Silverdale, Newcastle-under-Lyme, Staffordshire, ST5 6DH). The current analysis focuses on nearby housing transactions from 2015 to 2025.

## Repository layout

- `input/`: raw inputs and derived final distance file
- `output/`: intermediate outputs, caches, filtered lookups, summaries, and matched extracts
- `scripts/`: R pipeline scripts used to build, filter, enrich, and geocode the data
- `2_Build_Analysis_Dataset.Rmd`: combine all data for analysis

## Data sources

### Price Paid Data (PPD)

- Source: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
- Download strategy: yearly CSV files listed in `input/ppd_link.txt`
- Filtering is argument-driven via:
   - `filter_field` (for example: `district`, `county`)
   - `filter_values` (comma-separated and case-insensitive)
- Current default workflow filters districts:
   - `STOKE-ON-TRENT`
   - `NEWCASTLE-UNDER-LYME`
   - `STAFFORD`

### EPC

- Source: https://epc.opendatacommunities.org/downloads/domestic
- EPC files are controlled by `input/epc_filenames.txt`
- Each line can be either:
   - a domestic folder path (the script appends `certificates.csv`), or
   - a direct path to a `certificates.csv` file
- Current file list includes:
   - `input/domestic-E06000021-Stoke-on-Trent`
   - `input/domestic-E07000195-Newcastle-under-Lyme`
   - `input/domestic-E07000197-Stafford`

### Postcode to LSOA lookup

- Input file: `input/PCD_OA21_LSOA21_MSOA21_LAD_AUG24_UK_LU.csv`
- Used to attach `lsoa21cd` to matched PPD rows via postcode

### LSOA boundaries

- Input file: `input/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW.geojson`
- Used to select LSOAs intersecting the desired location bounding box
- Downloaded from `https://geoportal.statistics.gov.uk/datasets/ons::lower-layer-super-output-areas-december-2021-boundaries-ew-bsc-v4-2/explore?location=52.846052%2C-2.465415%2C6` 
- Already filtered according to a desired bounding box. 

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
- Filters rows using a configurable field and values
- Writes `input/ppd_data.csv`

Run:

```bash
Rscript scripts/build_ppd_data.R input/ppd_link.txt input/ppd_data.csv district "STOKE-ON-TRENT,NEWCASTLE-UNDER-LYME,STAFFORD"
```

Notes:

- Argument 3 is `filter_field` (for example `district`, `county`).
- Argument 4 is a comma-separated list of values for that field.
- Filtering is case-insensitive.

### 2. Build EPC ↔ PPD mapping

Script: `scripts/build_epc_ppd_mapping.R`

Purpose:

- Reads EPC certificates and filtered PPD data
- Reads EPC certificates only from paths listed in `input/epc_filenames.txt`
- Normalizes postcode and address fields
- Produces exact and fuzzy mapping passes
- Writes `output/epc_ppd_mapping.csv` and `output/epc_ppd_mapping_summary.txt`

Run:

```bash
Rscript scripts/build_epc_ppd_mapping.R
```

### 3. Build bounding box from target location and prepare target LSOA codes

Script: `scripts/create_bbox_27700.R`

Purpose:

- Builds a bounding box in `EPSG:27700` from a source GeoJSON and a meter distance
- Prints bbox in both `EPSG:27700` and lon/lat (`EPSG:4326`)
- Used to define the area for extracting target LSOA codes

Run (Walleys Quarry Landfill, 9000 m):

```bash
Rscript scripts/create_bbox_27700.R input/landfill.geojson 9000
```

Then:

- Use the printed lon/lat bbox to extract the target LSOA/LAD code list from ONS Geoportal:
   - `https://geoportal.statistics.gov.uk/datasets/ons::lower-layer-super-output-areas-december-2021-boundaries-ew-bsc-v4-2/explore?location=52.846052%2C-2.465415%2C6`
- Save the extracted code layer as a GeoJSON file (for example `input/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW.geojson`).

### 4. Build matched outputs with target-LSOA filtering for PPD

Script: `scripts/build_lsoa_filtered_matched_data.R`

Purpose:

- Reads the mapping output
- Reads target codes from a GeoJSON file (default: `input/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW.geojson`)
- Loads EPC matches by `lmk_key` from source certificates and writes them unchanged (no row-level LSOA filtering)
- For matched PPD rows, attaches `lsoa21cd` via postcode from `input/PCD_OA21_LSOA21_MSOA21_LAD_AUG24_UK_LU.csv`
- Filters matched PPD rows to non-missing `lsoa21cd` that are in target `LSOA21CD` codes from the GeoJSON
- Writes a summary report with pre/post counts and missing/non-missing `lsoa21cd` counts
- Writes:
   - `output/matched_epc.csv`
   - `output/matched_ppd.csv`
   - `output/build_lsoa_filtered_matched_data_summary.txt`

Run:

```bash
Rscript scripts/build_lsoa_filtered_matched_data.R 
```

### 5. Build and geocode unique addresses

Scripts:

- `scripts/build_unique_geocode_addresses.R`
- `scripts/geocode_addresses_precise.R`

Purpose:

- Builds unique normalized address/postcode pairs from `output/matched_ppd.csv`
- Geocodes at address level (not postcode level)
- Supports ranged geocoding runs (for example `0-10000`) and cache reuse
- Writes:
   - `output/unique_address_geocode_input.csv`
   - `output/address_geocodes_*.csv`
   - `output/address_geocode_cache.csv`

Run:

```bash
Rscript scripts/build_unique_geocode_addresses.R

export MAPBOX_API_KEY='your_api_key_here'
Rscript scripts/geocode_addresses_precise.R output/unique_address_geocode_input.csv output/address_geocodes.csv output/address_geocode_cache.csv "$MAPBOX_API_KEY" 0-10000
```

### 6. Compute landfill distances from geocoded addresses (EPSG:27700)

Script: `scripts/compute_distances_27700.R`

Purpose:

- Reads target location geometry from `input/landfill.geojson`
- Reads geocoded address files via pattern (for example `output/address_geocodes*`)
- Computes address-to-target distance using `sf`
- Transforms coordinates to British National Grid `EPSG:27700`
- Writes:
   - `output/address_landfill_distances_27700.csv`

Run:

```bash
Rscript scripts/compute_distances_27700.R
```

### 7. Build analysis dataframe

Script/notebook: `Build_Analysis_Dataset.Rmd`

Purpose:

- Reads matched PPD, matched EPC, EPC↔PPD mapping, address mapping (`output/unique_address_geocode_input.csv`), and landfill distance output (`output/address_landfill_distances_27700.csv`)
- For each deed sale (`unique_id`), keeps the EPC record with date closest to `deed_date`:
   - EPC event date uses `lodgement_date` if available, else `inspection_date`
- Keeps only rows matched in both PPD and EPC via the mapping file
- Joins distance data by matching `normalized_address` + `normalized_postcode` through the address mapping table
- Keeps all matched PPD/EPC rows and appends distance columns where available (`distance_to_target_m`, `nearest_target_feature_index`)
- Uses `ppd_house_id` in the final dataframe as the address-level key
- Writes:
   - `output/analysis_dataframe.csv`
   - `output/analysis_dataframe_summary.txt`
   - `output/analysis_dataframe_distance_by_deed_year.txt`

Run (from R):

```r
rmarkdown::render("Build_Analysis_Dataset.Rmd")
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

### EPC ↔ PPD Mapping

From `output/epc_ppd_mapping_summary.txt`:

- EPC rows indexed: 238,971
- PPD rows indexed: 99,055
- Matched rows in mapping: 119,495
- Distinct matched keys: 66,639
- PPD matching rate (distinct matched PPD keys / distinct PPD keys): 85.55%
- PPD row matching rate (matched PPD rows / PPD rows indexed): 86.24%
- Distinct matched EPC certificates: 91,187
- Distinct matched PPD transactions: 85,422
- Skipped postcode groups in fuzzy passes: 14

### LSOA Filter

From `output/build_lsoa_filtered_matched_data_summary.txt`:

**Mapping Input**

- **Mapping rows (input):** 119,495
- **Distinct LMK_KEY in mapping:** 91,187
- **Distinct PPD IDs in mapping:** 85,422
- **Distinct target LSOA21CD values:** 222

**Postcode Lookup Coverage**

- **Postcode lookup rows (input):** 2,706,710
- **Rows with missing `lsoa21cd` (pre-match):** 240,021
- **Rows with non-missing `lsoa21cd` (pre-match):** 2,466,689
- **Postcode lookup rows (deduped by `postcode_key`):** 2,706,710
- **Distinct `postcode_key` in lookup:** 2,706,710

**PPD within Target LSOAs**

- **Matched PPD rows with non-missing `lsoa21cd` (pre-keep):** 85,400
- **PPD postcodes without attached `lsoa21cd`:** 5
- **Matched PPD rows kept (non-missing and within target `LSOA21CD`):** 55,633

### Count of PPD per Deed Year and Distance

Number of houses by `deed_year`, with distance bands as columns.

| Deed Year | <2km | 2-5km | 5-10km | >10km |
|---|---:|---:|---:|---:|
| 2015 | 402 | 1,438 | 2,848 | 26 |
| 2016 | 404 | 1,568 | 3,084 | 23 |
| 2017 | 387 | 1,588 | 3,371 | 36 |
| 2018 | 407 | 1,631 | 3,351 | 29 |
| 2019 | 400 | 1,635 | 3,254 | 35 |
| 2020 | 381 | 1,344 | 2,830 | 35 |
| 2021 | 559 | 1,885 | 3,666 | 34 |
| 2022 | 494 | 1,616 | 3,251 | 35 |
| 2023 | 439 | 1,346 | 2,888 | 15 |
| 2024 | 450 | 1,485 | 2,912 | 21 |
| 2025 | 317 | 1,229 | 2,455 | 29 |

### Average Sold Amount by Deed Year and Distance

Average sold amount by `deed_year`, with distance bands as columns.

| Deed Year | <2km | 2-5km | 5-10km | >10km |
|---|---:|---:|---:|---:|
| 2015 | 151,461.1 | 118,267.0 | 116,430.1 | 225,403.6 |
| 2016 | 157,825.2 | 124,401.6 | 122,238.8 | 245,284.8 |
| 2017 | 161,915.9 | 134,476.7 | 125,371.4 | 257,832.6 |
| 2018 | 184,053.2 | 135,044.4 | 130,950.3 | 318,256.9 |
| 2019 | 170,681.9 | 139,605.5 | 132,710.4 | 329,394.3 |
| 2020 | 183,576.7 | 151,017.9 | 138,982.7 | 295,020.6 |
| 2021 | 185,799.1 | 166,404.9 | 152,320.4 | 429,866.0 |
| 2022 | 204,041.7 | 165,214.1 | 155,019.2 | 294,958.4 |
| 2023 | 208,905.7 | 166,541.3 | 155,857.1 | 315,366.7 |
| 2024 | 196,157.2 | 173,218.6 | 165,949.8 | 366,818.9 |
| 2025 | 203,108.0 | 178,745.5 | 166,316.0 | 208,191.0 |

## Main outputs

- `input/ppd_data.csv`: filtered PPD source table
- `output/epc_ppd_mapping.csv`: EPC/PPD mapping table
- `output/epc_ppd_mapping_summary.txt`: mapping summary
- `output/matched_epc.csv`: matched EPC rows (no row-level LSOA filtering)
- `output/matched_ppd.csv`: matched PPD rows filtered to non-missing target `lsoa21cd`
- `output/build_lsoa_filtered_matched_data_summary.txt`: counts from matched build and PPD LSOA filtering
- `output/unique_address_geocode_input.csv`: unique normalized address/postcode input for address-level geocoding
- `output/address_geocodes_*.csv`: address-level geocode outputs by processed range (for example `output/address_geocodes_0-10000.csv`)
- `output/address_geocode_cache.csv`: address geocode cache (query-level)
- `output/address_landfill_distances_27700.csv`: address-to-landfill distances (`EPSG:27700`, meters)
- `output/analysis_dataframe.csv`: final analysis dataframe (matched PPD+EPC with distance fields appended)
- `output/analysis_dataframe_summary.txt`: analysis summary text
- `output/analysis_dataframe_distance_by_deed_year.txt`: yearly house counts by distance bands (`<2km`, `2-5km`, `5-10km`, `>10km`)
