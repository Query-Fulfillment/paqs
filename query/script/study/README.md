# Study Functions Documentation

## Table of Contents

1. [get_geog_info](#get_geog_info)
2. [map_geographic_variables](#map_geographic_variables)
3. [get_valid_geog](#get_valid_geog)
4. [get_payer_info](#get_payer_info)
5. [get_bmi](#get_bmi)
6. [get_demographic](#get_demographic)
7. [define_criteria.death](#define_criteriadeath)

---

## Overview

This README provides concise documentation for the core study‑specific functions located in `query/script/study/`. Each function is exported for use in query pipelines and includes parameter descriptions, return values, and usage examples.

---

## Function Reference

### `get_geog_info`

```r
get_geog_info(
  cohort = NULL,
  end_date = "2024-12-31",
  lookback_years = 5
)
```

* **Purpose** – Retrieve the most recent address for each patient, handling fall‑backs for missing ZIP codes.
* **Parameters**
  * `cohort` – Optional tibble with `patid` to limit the lookup.
  * `end_date` – Upper bound of the look‑back period (default "2024-12-31").
  * `lookback_years` – Number of years to look back from `end_date` (default 5).
* **Return** – Validated cohort tibble containing `patid`, `address_zip5`, `address_state`, and address‑period columns.
* **Example**

```r
latest_addr <- get_geog_info(cohort = my_cohort)
```

---

### `map_geographic_variables`

```r
map_geographic_variables(
  address_tbl,
  geo_var = c("adi", "ruca", "state")
)
```

* **Purpose** – Join geographic mapping tables (ADI, RUCA, or state) to an address tibble.
* **Parameters**
  * `address_tbl` – Tibble with `patid` and location identifiers (`address_zip5` or `address_state`).
  * `geo_var` – One of "adi", "ruca", or "state" indicating the mapping to apply.
* **Return** – Tibble with `patid` and the mapped variable (`adi_quartile`, `ruca_code`, or `state_name`).
* **Example**

```r
adi_tbl <- map_geographic_variables(address_tbl = latest_addr, geo_var = "adi")
```

---

### `get_valid_geog`

```r
get_valid_geog(
  cohort = NULL
)
```

* **Purpose** – Flag records with valid ZIP and state values.
* **Parameters**
  * `cohort` – Cohort tibble containing `address_zip5` and `address_state`.
* **Return** – Tibble with boolean flags: `valid_zip`, `valid_state`, `valid_zip_or_state`, `valid_zip_and_state`.
* **Example**

```r
geog_flags <- get_valid_geog(cohort = latest_addr)
```

---

### `get_payer_info`

```r
get_payer_info(
  cohort,
  cohort_encounterid_col
)
```

* **Purpose** – Summarize primary payer types for each patient and assign ranking categories.
* **Parameters**
  * `cohort` – Tibble with patient identifiers.
  * `cohort_encounterid_col` – Column name in `cohort` that holds the encounter identifier.
* **Return** – Validated cohort tibble with columns `payer_cat`, `payer_rank`, and associated encounter information.
* **Example**

```r
payer_tbl <- get_payer_info(cohort = my_cohort, cohort_encounterid_col = "encounterid")
```

---

### `get_bmi`

```r
get_bmi(
  cohort,
  vital_tbl = cdm_tbl('vital'),
  demographic_tbl = cdm_tbl('demographic'),
  cohort_date_col,
  adult_wt_days = -365L,
  adult_ht_days = -3650L,
  child_ht_days = -90L,
  child_wt_days = -90L,
  infant_ht_days = -10L,
  infant_wt_days = -10L
)
```

* **Purpose** – Compute BMI categories for infants, children, and adults using the most recent height/weight measurements.
* **Parameters**
  * `cohort` – Patient cohort with `patid` and an index/anchor date.
  * `vital_tbl` – Vital signs table (default `cdm_tbl('vital')`).
  * `demographic_tbl` – Demographics table (default `cdm_tbl('demographic')`).
  * `cohort_date_col` – Column in `cohort` representing the index date.
  * Look‑back day arguments – Negative integers defining how far back to search for measurements.
* **Return** – Tibble with `patid`, `bmi_category`, and age group (`bmi_age`).
* **Example**

```r
bmi_results <- get_bmi(
  cohort = my_cohort,
  cohort_date_col = "index_date"
)
```

---

### `get_demographic`

```r
get_demographic(
  cohort = NULL,
  end_date = NULL,
  ce_date = NULL
)
```

* **Purpose** – Pull demographic attributes (age, sex, race, ethnicity) and compute age categories.
* **Parameters**
  * `cohort` – Optional tibble to join on `patid`.
  * `end_date` – End date for age calculation (optional).
  * `ce_date` – Column name for enrollment date used to extract year/month components.
* **Return** – Validated tibble with `patid`, age, `sex_label`, `race_label`, `hispanic_label`, and enrollment date components.
* **Example**

```r
demo_tbl <- get_demographic(cohort = my_cohort, end_date = "2023-12-31")
```

---

### `define_criteria.death`

```r
define_criteria.death(
  cohort = NULL,
  codeset = NULL,
  start_date,
  end_date,
  min_codes_required = 1,
  min_days_separation = 0,
  qualifying_event = "first",
  criterion_suffix
)
```

* **Purpose** – Filter death records for a cohort, optionally joining to an existing cohort.
* **Parameters** – Same as `define_criteria` (see base README) with `cohort` optional.
* **Return** – Tibble with `patid` and a death date column named `criterion_<suffix>_date`.
* **Example**

```r
death_cohort <- define_criteria.death(
  start_date = "2020-01-01",
  end_date = "2023-12-31",
  criterion_suffix = "death"
)
```

---

## Additional Notes

* All functions rely on the `cdm_tbl()` abstraction and return **validated** cohort tables via `validate_final_cohort()`, ensuring consistent downstream usage.
* The `echo_text()` calls provide runtime logging for debugging within the framework.
