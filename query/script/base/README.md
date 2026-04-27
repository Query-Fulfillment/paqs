# define_criteria Function Documentation

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Function Reference](#function-reference)
4. [User Guide](#user-guide)
5. [Examples](#examples)
6. [Supported Code Types](#supported-code-types)
7. [Troubleshooting](#troubleshooting)
8. [Advanced Usage](#advanced-usage)

---

## Overview

The `define_criteria` function is a flexible, S3-dispatched method for defining clinical cohort criteria across multiple CDM tables. It supports both the **PCORnet Common Data Model (CDM)** and **OMOP CDM**, enabling filtering of patients based on diagnosis codes, procedure codes, medications, lab results, and other clinical events with sophisticated temporal and frequency requirements.

### Key Features

- **Multi-CDM support**: Works with both PCORnet and OMOP CDMs, configured via `set_cdm_config()`
- **Multi-table support**: Works with all major CDM tables; codesets spanning multiple tables are handled automatically
- **Flexible date handling**: Accepts Date objects, date strings, column references, or `NULL` (no bound)
- **Temporal requirements**: Support for minimum codes required and days separation
- **Event selection**: Choose first, last, random, or all qualifying events
- **Encounter type filtering**: Restrict events to specific encounter types via `enc_type_fil`
- **Column retention**: Carry codeset or cohort columns through to output with `retain_codeset_cols` and `retain_cohort_cols`
- **Event grouping**: Apply qualification logic within user-defined subgroups via `event_group_cols`
- **Wildcard codeset support**: Use `code = "*"` to match all codes of a given codetype (PCORnet only)
- **Extensible design**: Easy to add custom logic for specific table types via S3 dispatch

---

## Prerequisites

Before calling `define_criteria`, you must configure the CDM type for the session. This sets global table configurations used internally throughout all criteria functions.

```r
# Set CDM type — must be called once before any define_criteria calls
set_cdm_config("pcornet")   # for PCORnet CDM
set_cdm_config("omop")      # for OMOP CDM
```

`set_cdm_config()` populates `.GlobalEnv$cdm_type` and `.GlobalEnv$TABLE_CONFIGS`. Failing to call this before `define_criteria` will result in errors when the function attempts to resolve table and column configurations.

---

## Function Reference

### Main Function

```r
define_criteria(
  cohort = NULL,
  codeset,
  start_date = NULL,
  end_date = NULL,
  min_codes_required = 1,
  min_days_separation = 0,
  qualifying_event = "first",
  criterion_suffix,
  enc_type_fil = NULL,
  multi_table_scope = c("post_union", "per_table", "both"),
  retain_codeset_cols = NULL,
  retain_cohort_cols = NULL,
  event_group_cols = NULL
)
```

### Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `cohort` | data.frame/tibble/remote table | No | `NULL` | Optional existing cohort to filter. Must contain `patid` column |
| `codeset` | data.frame/tibble/remote table | **Yes** | - | Codeset with `codetype` and `code` columns |
| `start_date` | Date/character/NULL | No | `NULL` | Start date for analysis period. `NULL` means no lower bound |
| `end_date` | Date/character/NULL | No | `NULL` | End date for analysis period. `NULL` means no upper bound |
| `min_codes_required` | integer | No | `1` | Minimum number of distinct events required |
| `min_days_separation` | integer | No | `0` | Minimum days between first and last event |
| `qualifying_event` | character | No | `"first"` | Which event to return: `"first"`, `"last"`, `"random"`, or `"all"` |
| `criterion_suffix` | character | **Yes** | - | Suffix for output column names |
| `enc_type_fil` | character vector | No | `NULL` | Filter to restrict events to specific encounter types (PCORnet only). For `diagnosis` and `procedures` tables, filters directly on `enc_type`; for all other tables, joins the `encounter` table to apply the filter |
| `multi_table_scope` | character | No | `"post_union"` | Controls how qualification logic is applied when a codeset spans multiple tables. See [Multi-Table Codesets](#multi-table-codesets) |
| `retain_codeset_cols` | character vector | No | `NULL` | Additional columns from `codeset` to carry through to the output. Must not overlap with `retain_cohort_cols` |
| `retain_cohort_cols` | character vector | No | `NULL` | Additional columns from `cohort` to carry through to the output. Requires `cohort` to be non-NULL. Must not overlap with `retain_codeset_cols` |
| `event_group_cols` | character vector | No | `NULL` | Columns by which to group event-level qualification logic (e.g., distinct event counts and days separation are evaluated within each group). All columns listed here must also appear in `retain_codeset_cols` or `retain_cohort_cols` |

### Note on `min_codes_required` and `min_days_separation`

When `min_codes_required` is set to **1**, the function automatically forces `min_days_separation` to **0**. This ensures that a single qualifying event is always returned regardless of any day-separation requirement that may have been supplied.

---

### Return Value

Returns a tibble with the following columns:

- `patid`: Patient identifier
- `encounterid_{criterion_suffix}`: Encounter identifier for the qualifying event
- `criterion_{criterion_suffix}_date`: Date of the qualifying event
- `enc_type_{criterion_suffix}`: Encounter type (present when `enc_type_fil` is supplied, or when the source table natively includes `enc_type`)
- `event_code_{criterion_suffix}`: The qualifying code value from the matched event (standardized column name regardless of source table)
- Any columns specified in `retain_codeset_cols` or `retain_cohort_cols`

> **Note**: The code value column is always named `event_code_{criterion_suffix}` in the output, regardless of the underlying CDM column name (e.g., `dx`, `px`, `ndc`, `rxnorm_cui`).

When `multi_table_scope = "both"` is used, the return value is a named **list** with two elements: `post_union` and `per_table`, each containing a tibble as described above.

---

## User Guide

### Basic Workflow

1. **Configure CDM**: Call `set_cdm_config("pcornet")` or `set_cdm_config("omop")` once per session
2. **Prepare your codeset**: Create a tibble with `codetype` and `code` columns
3. **Define temporal parameters**: Set date range and frequency requirements
4. **Call the function**: Execute with your parameters
5. **Use the results**: Join with other criteria or use for analysis

### Codeset Format

Your codeset must be a data frame/tibble with these required columns:

```r
# Example codeset structure
diabetes_codes <- tibble(
  codetype = c("DX10", "DX10", "DX09"),
  code = c("E11.9", "E10.9", "250.00"),
  description = c("Type 2 diabetes", "Type 1 diabetes", "Diabetes mellitus") # optional
)
```

### Wildcard Codesets (PCORnet only)

You can use `code = "*"` to match all codes of a given codetype without specifying individual codes. The function detects this and filters by `dx_type` (or equivalent type column) rather than joining on specific code values.

```r
# Match all ICD-10 diagnoses — no specific codes needed
all_dx10 <- tibble(
  codetype = "DX10",
  code = "*"
)

result <- define_criteria(
  codeset = all_dx10,
  start_date = "2022-01-01",
  end_date = "2022-12-31",
  criterion_suffix = "any_dx10"
)
```

> **Note**: `retain_codeset_cols` is not supported when using wildcard codesets and will produce an error if supplied.

### Date Input Formats

The function accepts multiple date formats:

```r
# Date objects
start_date = as.Date("2021-01-01")

# Character strings (multiple formats supported)
start_date = "2021-01-01"      # ISO format
start_date = "01/01/2021"      # US format
start_date = "01-01-2021"      # Hyphenated
start_date = "January 1, 2021" # Written format

# Column references (for dynamic, patient-level dates)
start_date = "criterion_cancer_dx_date"   # References a column in your data

# NULL (no bound)
start_date = NULL   # No lower date restriction
end_date = NULL     # No upper date restriction
```

When both `start_date` and `end_date` are `NULL`, the function analyzes all available data and emits an informational message to confirm no date restrictions are applied.

### Multi-Table Codesets

When a codeset contains codetypes that map to more than one CDM table (e.g., both `DX10` and `PR00`), the function automatically dispatches to `define_criteria.multi()`. The `multi_table_scope` parameter controls how qualification logic is applied across the unioned event streams:

| `multi_table_scope` | Behavior |
|---|---|
| `"post_union"` (default) | Events from all tables are pooled first, then `min_codes_required` and `min_days_separation` are applied across the combined pool |
| `"per_table"` | Qualification criteria are applied separately within each table; a patient qualifies if they meet the criteria in **any** single table; results are unioned |
| `"both"` | Both strategies are run; returns a named list with `$post_union` and `$per_table` results |

```r
# Codeset spanning both diagnosis and procedure tables
combo_codes <- tibble(
  codetype = c("DX10", "PX10"),
  code     = c("E11.9", "0DT70ZZ")
)

# Qualify patients with 2+ events across EITHER table
result_post_union <- define_criteria(
  codeset           = combo_codes,
  start_date        = "2021-01-01",
  end_date          = "2023-12-31",
  min_codes_required = 2,
  multi_table_scope = "post_union",
  criterion_suffix  = "combo"
)

# Get both strategies at once
result_both <- define_criteria(
  codeset           = combo_codes,
  start_date        = "2021-01-01",
  end_date          = "2023-12-31",
  min_codes_required = 2,
  multi_table_scope = "both",
  criterion_suffix  = "combo"
)

result_both$post_union
result_both$per_table
```

---

## Examples

### Example 1: Basic Diagnosis Criteria

```r
set_cdm_config("pcornet")

diabetes_codes <- tibble(
  codetype = c("DX10", "DX10", "DX09"),
  code = c("E11.9", "E10.9", "250.00")
)

diabetes_cohort <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  min_codes_required = 1,
  qualifying_event = "first",
  criterion_suffix = "diabetes"
)
```

### Example 2: Multiple Events with Time Separation

```r
# Require 2+ diabetes codes at least 30 days apart
diabetes_persistent <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  min_codes_required = 2,
  min_days_separation = 30,
  qualifying_event = "first",
  criterion_suffix = "diabetes_persistent"
)
```

### Example 3: Procedure Codes

```r
surgery_codes <- tibble(
  codetype = c("PX10", "PX09"),
  code = c("0DT70ZZ", "43.7")
)

surgery_cohort <- define_criteria(
  codeset = surgery_codes,
  start_date = "2021-01-01",
  end_date = "2021-12-31",
  min_codes_required = 1,
  qualifying_event = "last",  # Get most recent surgery
  criterion_suffix = "surgery"
)
```

### Example 4: Using an Existing Cohort

```r
existing_cohort <- tibble(patid = c("001", "002", "003"))

refined_cohort <- define_criteria(
  cohort = existing_cohort,
  codeset = diabetes_codes,
  start_date = "2022-01-01",
  end_date = "2022-12-31",
  criterion_suffix = "diabetes_2022"
)
```

### Example 5: Filtering by Encounter Type

```r
# Restrict to inpatient (IP) and emergency (EI) encounters only
inpatient_dx <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  enc_type_fil = c("IP", "EI"),
  criterion_suffix = "diabetes_inpatient"
)
```

### Example 6: Retaining Extra Columns

```r
# Add a 'category' column to the codeset and carry it through to the output
diabetes_codes_annotated <- tibble(
  codetype = c("DX10", "DX10", "DX09"),
  code = c("E11.9", "E10.9", "250.00"),
  category = c("T2DM", "T1DM", "T2DM")
)

result_with_category <- define_criteria(
  codeset = diabetes_codes_annotated,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  retain_codeset_cols = "category",
  criterion_suffix = "diabetes_typed"
)
# Output will include a 'category' column alongside standard result columns
```

### Example 7: Event Grouping

```r
# Qualify patients separately within each category group
result_grouped <- define_criteria(
  codeset = diabetes_codes_annotated,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  min_codes_required = 2,
  retain_codeset_cols = "category",
  event_group_cols = "category",   # must be listed in retain_codeset_cols
  criterion_suffix = "diabetes_by_type"
)
```

### Example 8: Retrieve All Qualifying Events

```r
# Return every qualifying event row rather than a single per-patient row
all_events <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  qualifying_event = "all",
  criterion_suffix = "diabetes_all_events"
)
```

### Example 9: No Date Restrictions

```r
# Analyze the full history without any date bounds
full_history <- define_criteria(
  codeset = diabetes_codes,
  start_date = NULL,
  end_date = NULL,
  criterion_suffix = "diabetes_full"
)
```

### Example 10: Multiple Criteria Workflow

```r
set_cdm_config("pcornet")
results <- list()

results$diabetes <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  min_codes_required = 2,
  min_days_separation = 30,
  criterion_suffix = "diabetes"
)

htn_codes <- tibble(codetype = "DX10", code = c("I10", "I11.0", "I11.9"))

results$hypertension <- define_criteria(
  cohort = results$diabetes,
  codeset = htn_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  criterion_suffix = "hypertension"
)

metformin_codes <- tibble(codetype = "RX11", code = c("6809", "861004"))

results$metformin <- define_criteria(
  cohort = results$diabetes,
  codeset = metformin_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  criterion_suffix = "metformin"
)
```

### Example 11: OMOP CDM

```r
set_cdm_config("omop")

omop_diabetes <- tibble(
  codetype = "DX10",
  code = c("201826", "201254")  # OMOP concept IDs
)

omop_result <- define_criteria(
  codeset = omop_diabetes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  criterion_suffix = "diabetes_omop"
)
```

---

## Supported Code Types

### PCORnet CDM — By Table

| Table | Supported Codetypes | Code Column | Primary Date | Fallback Date |
|---|---|---|---|---|
| **diagnosis** | DX09, DX10, DX11, DXSM | dx | dx_date | admit_date |
| **procedures** | PX09, PX10, PX11, PXCH, PXLC, PXND, PXRE | px | px_date | admit_date |
| **dispensing** | RX01, RX11, RX09 | ndc | dispense_date | dispense_date |
| **prescribing** | PR00 | rxnorm_cui | rx_order_date | rx_start_date |
| **lab_result_cm** | LBLC, LBCH, LB09, LB10, LB11 | lab_loinc | result_date | lab_order_date |
| **med_admin** | MA09, MA11, MA00 | medadmin_code | medadmin_start_date | medadmin_start_date |
| **obs_clin** | OCSM, OCLC | obsclin_code | obsclin_start_date | obsclin_stop_date |
| **immunization** | VXCX, VXND, VXCH, VXRX | vx_code | vx_admin_date | vx_record_date |
| **death** | DTH | death_cause_code | death_date | - |

### Code Type Descriptions

- **DX09/DX10/DX11**: ICD-9-CM, ICD-10-CM, ICD-11 diagnosis codes
- **DXSM**: SNOMED CT diagnosis codes
- **PX09/PX10/PX11**: ICD-9-CM, ICD-10-PCS, ICD-11 procedure codes
- **PXCH/PXLC/PXND/PXRE**: CPT, LOINC, NDC, Revenue procedure codes
- **RX01/RX11/RX09**: NDC, RxNorm drug codes
- **PR00**: RxNorm prescribing codes
- **LBLC/LBCH**: LOINC, CPT lab codes
- **LB09/LB10/LB11**: ICD-9, ICD-10, ICD-11 lab codes
- **MA09/MA11/MA00**: ICD-9, ICD-11, RxNorm med administration codes
- **OCSM/OCLC**: SNOMED CT, LOINC observation codes
- **VXCX/VXND/VXCH/VXRX**: CVX, NDC, CPT, RxNorm immunization codes
- **DTH**: Death cause codes

---

## Troubleshooting

### Common Errors and Solutions

#### Error: `"codeset must contain a column named 'codetype'"`

Ensure your codeset has both `codetype` and `code` columns:

```r
codeset <- tibble(
  codetype = c("DX10", "DX10"),
  code = c("E11.9", "E10.9")
)
```

#### Error: `"No valid codetype found in codeset"`

Check that your codetypes match the supported values for your configured CDM. See [Supported Code Types](#supported-code-types).

#### Error: `"start_date must be before end_date"`

Verify your date range:

```r
# Correct
start_date = "2020-01-01"
end_date = "2023-12-31"

# Incorrect
start_date = "2023-12-31"
end_date = "2020-01-01"
```

#### Error: `"min_codes_required must be a positive integer >= 1"`

```r
# Correct
min_codes_required = 2
min_days_separation = 30

# Incorrect
min_codes_required = 0      # Must be >= 1
min_days_separation = -10   # Must be >= 0
```

#### Error: `"qualifying_event must be one of: 'first', 'last', 'random' or 'all'"`

Valid values are `"first"`, `"last"`, `"random"`, and `"all"`. Any other string will throw this error.

#### Error: `"Retained codeset columns and retained cohort columns must be uniquely named"`

Column names in `retain_codeset_cols` and `retain_cohort_cols` must not overlap. Rename conflicting columns in your codeset or cohort before calling the function.

#### Error: `"retain_codeset_cols was supplied, but cohort is NULL"` / missing column errors

When supplying `retain_cohort_cols`, a non-NULL `cohort` must be provided. All column names listed in `retain_codeset_cols` must exist in `codeset`, and all names in `retain_cohort_cols` must exist in `cohort`.

#### Error: `"event_group_cols must be retained in the event stream"`

Every column listed in `event_group_cols` must also appear in `retain_codeset_cols` or `retain_cohort_cols`. Add the missing column(s) to the appropriate retain argument.

#### Error: `"retain_codeset_cols is not supported when using wildcard code='*'"`

Wildcard codesets match by type rather than by joining on individual codes, so codeset columns other than `codetype` and `code` are not available in the event stream. Remove `retain_codeset_cols` or switch to an explicit codeset.

#### Warning: `"No patients qualified the cohort definition"`

1. Check if your codes exist in the database
2. Verify your date range includes relevant data
3. Relax `min_codes_required` or `min_days_separation`
4. Confirm `set_cdm_config()` was called with the correct CDM type

### Performance Tips

1. **Use existing cohorts**: Pass a `cohort` parameter to limit the search space before querying CDM tables
2. **Consider indexes**: Ensure CDM tables have indexes on `patid` and date columns
3. **Prefer `post_union`**: For multi-table codesets, `"post_union"` requires fewer passes over the data than `"both"`

### Debugging Steps

1. **Verify CDM configuration**:

   ```r
   .GlobalEnv$cdm_type         # Should be "pcornet" or "omop"
   names(.GlobalEnv$TABLE_CONFIGS)  # Should list expected tables
   ```

2. **Check your codeset**:

   ```r
   print(codeset)
   table(codeset$codetype)
   ```

3. **Verify date formats**:

   ```r
   resolve_date_input(start_date)  # Should not error
   resolve_date_input(end_date)
   ```

4. **Test with relaxed criteria**:

   ```r
   test_result <- define_criteria(
     codeset = your_codeset,
     start_date = NULL,
     end_date = NULL,
     min_codes_required = 1,
     criterion_suffix = "test"
   )
   ```

---

## Advanced Usage

### Column Retention and Event Grouping

`retain_codeset_cols` and `retain_cohort_cols` allow you to propagate extra columns from the input data into the output result. This is particularly useful when your codeset carries metadata (e.g., a drug category or diagnosis group label) that you want to analyze alongside the qualifying event.

`event_group_cols` takes this further by stratifying all qualification logic (distinct event counts, days separation, event selection) within the groups defined by those columns. Every column in `event_group_cols` must first be declared in `retain_codeset_cols` or `retain_cohort_cols`.

```r
annotated_meds <- tibble(
  codetype = c("RX11", "RX11", "PR00"),
  code     = c("6809", "861004", "2200644"),
  drug_class = c("biguanide", "biguanide", "sulfonylurea")
)

result <- define_criteria(
  codeset            = annotated_meds,
  start_date         = "2020-01-01",
  end_date           = "2023-12-31",
  min_codes_required = 2,
  retain_codeset_cols = "drug_class",
  event_group_cols   = "drug_class",   # qualify 2+ events within each drug class
  criterion_suffix   = "diabetes_meds"
)
```

### Dynamic Date Windows

By passing a column name as `start_date` or `end_date`, you can define patient-level time windows anchored to a prior criterion:

```r
# Only look for metformin AFTER the diabetes diagnosis date
results$metformin_post_dx <- define_criteria(
  cohort     = results$diabetes,
  codeset    = metformin_codes,
  start_date = "criterion_diabetes_date",  # column in results$diabetes
  end_date   = "2023-12-31",
  criterion_suffix = "metformin_post_dx"
)
```

### S3 Dispatch

`define_criteria` uses S3 dispatch on the class of `codeset` to route to table-specific methods. The main function assigns a class based on the tables detected in the codeset crosswalk before calling `UseMethod()`. Most tables delegate to `define_criteria.generic()`. You can override behavior for a specific table by implementing a named method:

---

This documentation provides comprehensive guidance for using the `define_criteria` function effectively. For additional function to characterize a developed cohort, refer to [study functions](https://github.com/Query-Fulfillment/paqs/blob/main/query/script/study/README.md)
