# define_criteria Function Documentation

## Table of Contents

1. [Overview](#overview)
2. [Function Reference](#function-reference)
3. [User Guide](#user-guide)
4. [Examples](#examples)
5. [Supported Code Types](#supported-code-types)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Usage](#advanced-usage)

## Overview

The `define_criteria` function is a flexible, S3-dispatched method for defining clinical cohort criteria across multiple PCORnet Common Data Model (CDM) tables. It supports filtering patients based on diagnosis codes, procedure codes, medications, lab results, and other clinical events with sophisticated temporal and frequency requirements.

### Key Features

- **Multi-table support**: Works with all major PCORnet CDM tables
- **Flexible date handling**: Accepts Date objects, date strings, or column references
- **Temporal requirements**: Support for minimum codes required and days separation
- **Event selection**: Choose first, last, or random qualifying events
- **Extensible design**: Easy to add custom logic for specific table types

---

## Function Reference

### Main Function

```r
define_criteria(
  cohort = NULL,
  codeset,
  start_date,
  end_date,
  min_codes_required = 1,
  min_days_separation = 0,
  qualifying_event = "first",
  criterion_suffix
)
```

### Parameters

| Parameter               | Type                           | Required      | Default     | Description                                                       |
| ----------------------- | ------------------------------ | ------------- | ----------- | ----------------------------------------------------------------- |
| `cohort`              | data.frame/tibble/remote table | No            | `NULL`    | Optional existing cohort to filter. Must contain `patid` column |
| `codeset`             | data.frame/tibble/remote table | **Yes** | -           | Codeset with `codetype` and `code` columns                    |
| `start_date`          | Date/character                 | **Yes** | -           | Start date for analysis period                                    |
| `end_date`            | Date/character                 | **Yes** | -           | End date for analysis period                                      |
| `min_codes_required`  | integer                        | No            | `1`       | Minimum number of distinct events required                        |
| `min_days_separation` | integer                        | No            | `0`       | Minimum days between first and last event                         |
| `qualifying_event`    | character                      | No            | `"first"` | Which event to return: "first", "last", or "random"               |
| `criterion_suffix`    | character                      | **Yes** | -           | Suffix for output column names                                    |

### Return Value

Returns a tibble with the following columns:

- `patid`: Patient identifier
- `encounterid_{criterion_suffix}`: Encounter identifier for the qualifying event
- `criterion_{criterion_suffix}_date`: Date of the qualifying event

---

## User Guide

### Basic Workflow

1. **Prepare your codeset**: Create a tibble with `codetype` and `code` columns
2. **Define temporal parameters**: Set date range and frequency requirements
3. **Call the function**: Execute with your parameters
4. **Use the results**: Join with other criteria or use for analysis

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

### Date Input Formats

The function accepts multiple date formats:

```r
# Date objects
start_date = as.Date("2021-01-01")

# Character strings (multiple formats supported)
start_date = "2021-01-01"    # ISO format
start_date = "01/01/2021"    # US format  
start_date = "01-01-2021"    # Hyphenated
start_date = "January 1, 2021" # Written format

# Column references (for dynamic dates)
start_date = "criterion_cancer_dx_date"    # References a column in your data
```

---

## Examples

### Example 1: Basic Diagnosis Criteria

```r
# Define diabetes diagnosis criteria
diabetes_codes <- tibble(
  codetype = c("DX10", "DX10", "DX09"),
  code = c("E11.9", "E10.9", "250.00")
)

diabetes_cohort <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31", 
  min_codes_required = 1,
  min_days_separation = 0,
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
# Define surgery criteria
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

### Example 4: Using Existing Cohort

```r
# Filter existing cohort for new criteria
existing_cohort <- tibble(patid = c("001", "002", "003"))

refined_cohort <- define_criteria(
  cohort = existing_cohort,
  codeset = diabetes_codes,
  start_date = "2022-01-01",
  end_date = "2022-12-31", 
  criterion_suffix = "diabetes_2022"
)
```

### Example 5: Lab Results with Custom Logic

```r
# HbA1c lab codes
hba1c_codes <- tibble(
  codetype = "LBLC",
  code = "4548-4"
)

hba1c_cohort <- define_criteria(
  codeset = hba1c_codes,
  start_date = "2021-01-01",
  end_date = "2023-12-31",
  min_codes_required = 2,
  min_days_separation = 90,  # At least 90 days between tests
  qualifying_event = "last",
  criterion_suffix = "hba1c",
  # Lab-specific parameters (if using lab_result_cm method)
  lab_value_filter = list(min_value = 7.0),
  normal_range_only = FALSE
)
```

### Example 6: Multiple Criteria Workflow

```r
# Build complex cohort with multiple criteria
results <- list()

# Step 1: Diabetes diagnosis
results$diabetes <- define_criteria(
  codeset = diabetes_codes,
  start_date = "2020-01-01", 
  end_date = "2023-12-31",
  min_codes_required = 2,
  min_days_separation = 30,
  criterion_suffix = "diabetes"
)

# Step 2: Hypertension (using diabetes cohort)
htn_codes <- tibble(codetype = "DX10", code = c("I10", "I11.0", "I11.9"))

results$hypertension <- define_criteria(
  cohort = results$diabetes,  # Only look in diabetes patients
  codeset = htn_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31", 
  criterion_suffix = "hypertension"
)

# Step 3: Medication exposure
metformin_codes <- tibble(codetype = "RX11", code = c("6809", "861004"))

results$metformin <- define_criteria(
  cohort = results$diabetes,
  codeset = metformin_codes,
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  criterion_suffix = "metformin"
)
```

---

## Supported Code Types

### By PCORnet CDM Table

| Table                   | Supported Codetypes                      | Code Column      | Primary Date        | Fallback Date |
| ----------------------- | ---------------------------------------- | ---------------- | ------------------- | ------------- |
| **diagnosis**     | DX09, DX10, DX11, DXSM                   | dx               | dx_date             | admit_date    |
| **procedure**     | PX09, PX10, PX11, PXCH, PXLC, PXND, PXRE | px               | px_date             | admit_date    |
| **dispensing**    | RX01, RX11, RX09                         | ndc              | dispense_date       | admit_date    |
| **prescribing**   | PR00                                     | rxnorm_cui       | rx_order_date       | admit_date    |
| **lab_result_cm** | LBLC, LBCH, LB09, LB10, LB11             | lab_loinc        | lab_order_date      | admit_date    |
| **med_admin**     | MA09, MA11, MA00                         | medadmin_code    | medadmin_start_date | admit_date    |
| **obs_clin**      | OCSM, OCLC                               | obsclin_code     | obsclin_date        | admit_date    |
| **immunization**  | VXCX, VXND, VXCH, VXRX                   | vx_code          | vx_admin_date       | admit_date    |
| **death**         | DTH                                      | death_cause_code | death_date          | -             |

### Code Type Descriptions

- **DX09/DX10/DX11**: ICD-9-CM, ICD-10-CM, ICD-11 diagnosis codes
- **DXSM**: SNOMED CT diagnosis codes
- **PX09/PX10/PX11**: ICD-9-CM, ICD-10-PCS, ICD-11 procedure codes
- **PXCH/PXLC/PXND/PXRE**: CPT, LOINC, NDC, Revenue procedure codes
- **RX01/RX11/RX09**: NDC, RxNorm drug codes
- **LBLC/LBCH**: LOINC, CPT lab codes
- **MA09/MA11/MA00**:

---

## Troubleshooting

### Common Errors and Solutions

#### Error: "codeset must contain a column named 'codetype'"

**Solution**: Ensure your codeset has both `codetype` and `code` columns:

```r
# Correct format
codeset <- tibble(
  codetype = c("DX10", "DX10"),
  code = c("E11.9", "E10.9")
)
```

#### Error: "No valid codetype found in codeset"

**Solution**: Check that your codetypes match the supported values. See [Supported Code Types](#supported-code-types).

#### Error: "start_date must be before end_date"

**Solution**: Verify your date range:

```r
# Correct
start_date = "2020-01-01"
end_date = "2023-12-31"

# Incorrect  
start_date = "2023-12-31"
end_date = "2020-01-01"
```

#### Warning: "No patients qualified the cohort definition"

**Solutions**:

1. Check if your codes exist in the database
2. Verify your date range includes relevant data
3. Relax `min_codes_required` or `min_days_separation` parameters
4. Check for data quality issues

#### Error: "min_codes_required must be a positive integer >= 1"

**Solution**: Ensure numeric parameters are valid:

```r
# Correct
min_codes_required = 2
min_days_separation = 30

# Incorrect
min_codes_required = 0      # Must be >= 1
min_days_separation = -10   # Must be >= 0  
```

### Performance Tips

1. **Use existing cohorts**: Pass a `cohort` parameter to limit the search space
2. **Consider indexes**: Ensure your CDM tables have proper indexes on `patid` and date columns

### Debugging Steps

1. **Check your codeset**:

   ```r
   print(codeset)
   table(codeset$codetype)
   ```
2. **Verify date formats**:

   ```r
   resolve_date_input(start_date)  # Should not error
   resolve_date_input(end_date)
   ```
3. **Test with relaxed criteria**:

   ```r
   # Try minimal requirements first
   test_result <- define_criteria(
     codeset = your_codeset,
     start_date = "2020-01-01", 
     end_date = "2023-12-31",
     min_codes_required = 1,
     min_days_separation = 0,
     criterion_suffix = "test"
   )
   ```

---

This documentation provides comprehensive guidance for using the `define_criteria` function effectively. For additional questions or issues, please refer to the troubleshooting section or contact qf@pcornet.org.
