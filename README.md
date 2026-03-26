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

### EPC (Domestic)

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

### 3. Build bounding box from landfill GeoJSON and prepare target LSOA codes

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

### 5. Geocode retained postcodes

Script: `scripts/geocode_prison_distances.R`

Purpose:

- Reads `output/matched_ppd.csv`
- Geocodes one point per postcode, not one point per address
- Writes:
   - `output/postcode_geocodes.csv`
   - `output/property_geocode_cache.csv`

Run:

```bash
export GEOCODE_MAPS_API_KEY='your_api_key_here'
Rscript scripts/geocode_prison_distances.R
```

### 6. Compute prison distances from geocoded postcodes (EPSG:27700)

Script: `scripts/compute_prison_distances.R`

Purpose:

- Reads `output/postcode_geocodes.csv`
- Computes postcode-to-prison distance using `sf`
- Transforms coordinates to British National Grid `EPSG:27700`
- Joins postcode-level distances back onto matched property rows
- Writes:
   - `output/postcode_prison_distances.csv`
   - `input/property_prison_distance.csv`

Run:

```bash
Rscript scripts/compute_prison_distances.R
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

### Address geocoding and distance snapshot (For Updating)

- `output/unique_address_geocode_input.csv`: 43,308 unique normalized address/postcode rows
- Sum of `matched_ppd_count` in unique-address input: 55,633
- `output/address_geocode_cache.csv`: 16,451 rows (16,451 unique queries)
- Geocode cache status:
   - `ok`: 16,451
- `output/address_landfill_distances_27700.csv`: 622 rows
- Rows with non-missing `distance_to_target_m`: 622
- Rows with `distance_to_target_m <= 10,000`: 620

### Analysis snapshot (For Updating)

- `output/analysis_dataframe.csv`: 55,633 rows
- Distinct sales in analysis dataframe (`unique_id`): 55,633
- Distinct houses in analysis dataframe (`ppd_house_id`): 43,308
- Rows with non-missing `distance_to_target_m`: 2,067

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
