# Table configurations for different code types

#' Setting PCORnet or OMOP Table Configs
#'
#' @param cdm_type string value, `pcornet` | `omop`
#'
#' @returns
#'
#' @export
#' @examples
set_cdm_config <- function(cdm_type) {
  .GlobalEnv$cdm_type <- cdm_type
  
  .GlobalEnv$TABLE_CONFIGS <-
    if (cdm_type == 'pcornet') {
      list(
        diagnosis = list(
          table = "diagnosis",
          code_column = "dx",
          type_column = "dx_type",
          primary_date_column = "dx_date",
          fallback_date_column = "admit_date",
          permitted_codetype = c('DX09', 'DX10', 'DX11', 'DXSM')
        ),
        procedures = list(
          table = "procedures",
          code_column = "px",
          type_column = "px_type",
          primary_date_column = "px_date",
          fallback_date_column = "admit_date",
          permitted_codetype = c(
            'PX09',
            'PX10',
            'PX11',
            'PXCH',
            'PXLC',
            'PXND',
            'PXRE'
          )
        ),
        medication = list(
          dispensing = list(
            table = "dispensing",
            code_column = "ndc",
            primary_date_column = "dispense_date",
            fallback_date_column = "admit_date",
            permitted_codetype = c("RX01", "RX11", "RX09")
          ),
          prescribing = list(
            table = "prescribing",
            code_column = "rxnorm_cui",
            primary_date_column = "rx_order_date",
            fallback_date_column = "admit_date",
            permitted_codetype = c("PR00")
          ),
          med_admin = list(
            table = "med_admin",
            code_column = "medadmin_code",
            primary_date_column = "medadmin_start_date",
            fallback_date_column = "admit_date",
            permitted_codetype = c('MA09', 'MA11', 'MA00')
          ),
          primary_date_column = c("medication_date"),
          code_column = c("medication_code")
        ),
        lab_result_cm = list(
          table = "lab_result_cm",
          code_column = "lab_loinc",
          primary_date_column = "result_date",
          fallback_date_column = "lab_order_date",
          permitted_codetype = c('LBLC', 'LBCH', 'LB09', 'LB10', 'LB11')
        ),
        obs_clin = list(
          table = "obs_clin",
          code_column = "obsclin_code",
          primary_date_column = "obsclin_date",
          fallback_date_column = "admit_date",
          permitted_codetype = c('OCSM', 'OCLC')
        ),
        immunization = list(
          table = "immunization",
          code_column = "vx_code",
          primary_date_column = "vx_admin_date",
          fallback_date_column = "admit_date",
          permitted_codetype = c('VXCX', 'VXND', 'VXCH', 'VXRX')
        ),
        death = list(
          table = "death",
          code_column = "death_cause_code",
          primary_date_column = "death_date",
          fallback_date_column = "admit_date"
        )
      )
    } else {
      list(
        diagnosis = list(
          table = "condition_occurrence",
          code_column = "condition_concept_id",
          primary_date_column = "condition_start_date",
          fallback_date_column = NULL
        ),
        procedures = list(
          table = "procedure_occurrence",
          code_column = "procedure_concept_id",
          primary_date_column = "procedure_start_date",
          fallback_date_column = NULL
        ),
        prescribing = list(
          table = "drug_exposure",
          code_column = "drug_concept_id",
          primary_date_column = "drug_exposure_start_date",
          fallback_date_column = NULL
        )
      )
    }
}


# Input validation functions
#' Title
#'
#' @param codeset
#' @param start_date
#' @param end_date
#' @param min_codes_required
#' @param min_days_separation
#' @param qualifying_event
#' @param criterion_suffix
#'
#' @returns
#'
#' @export
#' @examples

validate_all_inputs <- function(
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix
) {
  # Validate codeset
  if (is.null(codeset)) {
    stop("codeset cannot be null")
  }
  
  if (!is.data.frame(codeset) && !is.tbl(codeset)) {
    stop("codeset must be a data frame or tibble")
  }
  
  if (pull(count(codeset)) == 0) {
    stop("codeset cannot be empty")
  }
  
  if (!"codetype" %in% colnames(codeset)) {
    stop("codeset must contain a column named 'codetype'")
  }
  
  if (!"code" %in% colnames(codeset)) {
    stop("codeset must contain a column named 'code'")
  }
  
  # Validate numeric parameters
  if (
    !is.numeric(min_codes_required) ||
    min_codes_required < 1 ||
    min_codes_required != as.integer(min_codes_required)
  ) {
    stop("min_codes_required must be a positive integer >= 1")
  }
  
  if (
    !is.numeric(min_days_separation) ||
    min_days_separation < 0 ||
    min_days_separation != as.integer(min_days_separation)
  ) {
    stop("min_days_separation must be a non-negative integer >= 0")
  }
  
  # Validate qualifying_event
  if (!qualifying_event %in% c("first", "last", "random", "all")) {
    stop("qualifying_event must be one of: 'first', 'last', 'random' or 'all'")
  }
  
  # Validate criterion_suffix
  if (
    !is.character(criterion_suffix) ||
    length(criterion_suffix) != 1 ||
    nchar(criterion_suffix) == 0
  ) {
    stop("criterion_suffix must be a non-empty character string")
  }
  
  # Validate date inputs (allow NULL values)
  if (!is.null(start_date)) {
    tryCatch(
      {
        resolve_date_input(start_date)
      },
      error = function(e) {
        stop(paste("Invalid start_date format:", e$message))
      }
    )
  }
  
  if (!is.null(end_date)) {
    tryCatch(
      {
        resolve_date_input(end_date)
      },
      error = function(e) {
        stop(paste("Invalid end_date format:", e$message))
      }
    )
  }
}

validate_date_range <- function(start_date, end_date) {
  # Skip validation if either date is NULL
  if (is.null(start_date) || is.null(end_date)) {
    return(invisible(NULL))
  }
  
  start_val <- resolve_date_input(start_date)
  end_val <- resolve_date_input(end_date)
  
  # Only validate if both are actual dates (not column references)
  if (inherits(start_val, "Date") && inherits(end_val, "Date")) {
    if (start_val >= end_val) {
      stop("start_date must be before end_date")
    }
  }
}

# Enhanced resolve_date_input to handle NULL
resolve_date_input <- function(x) {
  if (is.null(x)) {
    return(NULL)
  } else if (inherits(x, "Date")) {
    return(x)
  } else if (is.character(x)) {
    # Try parsing using multiple common formats
    parsed <- suppressWarnings(parse_date_time(
      x,
      orders = c(
        "Ymd",
        "mdY",
        "dmY",
        "Y-m-d",
        "m/d/Y",
        "d/m/Y",
        "Y/m/d",
        "Y.m.d",
        "B d, Y",
        "d B Y"
      )
    ))
    if (!is.na(parsed)) {
      return(as.Date(parsed))
    } else {
      return(sym(x)) # Must be a column name
    }
  } else if (is_symbol(x)) {
    return(x)
  } else {
    stop(
      "Date input must be either a Date object, date string, column name, or NULL"
    )
  }
}

# Enhanced codetype to table mapping
#' Title
#'
#' @returns
#'
#' @export
#' @examples
# match_codetype_to_table <- function() {
# 	dat <- tibble::tribble(
# 		~codetype,
# 		~table,
# 		~pcornet_vocab_type,
# 		# diagnosis
# 		'DX09',
# 		'diagnosis',
# 		"09",
# 		'DX10',
# 		'diagnosis',
# 		"10",
# 		'DX11',
# 		'diagnosis',
# 		"11",
# 		'DXSM',
# 		'diagnosis',
# 		"SM",
# 		# dispensing
# 		'RX01',
# 		'medication',
# 		"",
# 		'RX11',
# 		'medication',
# 		"",
# 		'RX09',
# 		'medication',
# 		"",
# 		# procedure
# 		'PX09',
# 		'procedures',
# 		"09",
# 		'PX10',
# 		'procedures',
# 		"10",
# 		'PX11',
# 		'procedures',
# 		"11",
# 		'PXCH',
# 		'procedures',
# 		"CH",
# 		'PXLC',
# 		'procedures',
# 		"LC",
# 		'PXND',
# 		'procedures',
# 		"ND",
# 		'PXRE',
# 		'procedures',
# 		"RE",
# 		# prescribing
# 		'PR00',
# 		'medication',
# 		"",
# 		# lab_result_cm
# 		'LBLC',
# 		'lab_result_cm',
# 		"LC",
# 		'LBCH',
# 		'lab_result_cm',
# 		"CH",
# 		'LB09',
# 		'lab_result_cm',
# 		"09",
# 		'LB10',
# 		'lab_result_cm',
# 		"10",
# 		'LB11',
# 		'lab_result_cm',
# 		"11",
# 		# med_admin
# 		'MA09',
# 		'medication',
# 		"",
# 		'MA11',
# 		'medication',
# 		"",
# 		'MA00',
# 		'medication',
# 		"",
# 		# obs_clin
# 		'OCSM',
# 		'obs_clin',
# 		"",
# 		'OCLC',
# 		'obs_clin',
# 		"",
# 		# immunization
# 		'VXCX',
# 		'immunization',
# 		"CX",
# 		'VXND',
# 		'immunization',
# 		"ND",
# 		'VXCH',
# 		'immunization',
# 		"CH",
# 		'VXRX',
# 		'immunization',
# 		"RX",
# 		# death
# 		'DTH',
# 		'death',
# 		""
# 	) %>%
# 		copy_to_new(df = dat, name = "crosswalks", overwrite = TRUE)
# 	}

get_table_config <- function(codeset) {
  codetype_mapping <- .GlobalEnv$codesets$crosswalk %>%
    inner_join(codeset, by = "codetype") %>%
    distinct(table) %>%
    pull()
  
  if (length(codetype_mapping) == 0) {
    stop("No valid codetype found in codeset")
  }
  
  if (length(codetype_mapping) > 1) {
    warning(
      "Multiple codetypes found in codeset. Using the first one for method dispatch."
    )
    codetype_mapping <- codetype_mapping[1]
  }
  
  table_name <- codetype_mapping
  
  if (!table_name %in% names(TABLE_CONFIGS)) {
    stop(sprintf("No configuration found for table: %s", table_name))
  }
  
  return(TABLE_CONFIGS[[table_name]])
}

#' Main define_criteria function with enhanced validation and dispatch
#'
#' @param cohort Optional cohort to filter patients
#' @param codeset A tibble containing codes and codetype
#' @param start_date Starting date for analysis (Date, string, or column name)
#' @param end_date Ending date for analysis (Date, string, or column name)
#' @param min_codes_required Minimum number of distinct codes required (default: 1)
#' @param min_days_separation Minimum days between first and last event (default: 0)
#' @param qualifying_event Which event to return: "first", "last", or "random" (default: "first")
#' @param criterion_suffix Suffix for output column names
#' @param enc_type_fil Filter vector to limit encounter types
#'
#' @return A tibble with patid, encounterid, and criterion date
#' @export
define_criteria <- function(
    cohort = NULL,
    codeset,
    start_date = NULL, # Now defaults to NULL
    end_date = NULL, # Now defaults to NULL
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  # Comprehensive input validation
  validate_all_inputs(
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix
  )
  
  validate_date_range(start_date, end_date)
  
  # Provide informative messaging about date filtering
  if (is.null(start_date) && is.null(end_date)) {
    message("No date restrictions applied - analyzing all available data")
  } else if (is.null(start_date)) {
    message(sprintf(
      "No start date restriction - analyzing data through %s",
      end_date
    ))
  } else if (is.null(end_date)) {
    message(sprintf(
      "No end date restriction - analyzing data from %s onward",
      start_date
    ))
  } else {
    message(sprintf("Analyzing data from %s to %s", start_date, end_date))
  }
  
  # Get table configuration for dispatch
  table_config <- get_table_config(codeset)
  table_name <- codesets$crosswalk %>%
    inner_join(codeset, by = "codetype") %>%
    distinct(table) %>%
    pull()
  
  # Set class for S3 dispatch
  class(codeset) <- c(table_name, class(codeset))
  
  # Log automatic adjustment for single code requirement
  if (min_codes_required == 1) {
    message(
      "min_codes_required is set to 1, automatically setting min_days_separation to 0"
    )
    min_days_separation <- 0
  }
  
  # Use S3 method dispatch
  UseMethod("define_criteria", codeset)
}

# Enhanced helper functions

#' Apply date filters with robust error handling
apply_date_filters <- function(
    cohort_data,
    table_config,
    start_date,
    end_date,
    criterion_suffix,
    enc_type_fil
) {
  primary_date_col <- table_config$primary_date_column
  fallback_date_col <- table_config$fallback_date_column
  event_code_col <- table_config$code_column
  
  coalesced_date_col_name <- paste0("criterion_", criterion_suffix, "_date")
  criterion_encounterid <- paste0("encounterid_", criterion_suffix)
  
  event_code_col_criterion_suffix <- paste0(
    event_code_col,
    '_',
    criterion_suffix
  )
  
  criterion_enc_type <- paste0("enc_type_", criterion_suffix)
  
  start_val <- resolve_date_input(start_date)
  end_val <- resolve_date_input(end_date)
  
  # Helper function to create date filter conditions
  create_date_filter <- function(data, date_col) {
    conditions <- list()
    
    if (!is.null(start_val)) {
      conditions <- append(
        conditions,
        list(expr(!!sym(date_col) >= !!start_val))
      )
    }
    
    if (!is.null(end_val)) {
      conditions <- append(conditions, list(expr(!!sym(date_col) <= !!end_val)))
    }
    
    # If no date filters, return data as-is
    if (length(conditions) == 0) {
      return(data)
    }
    
    # Combine conditions with & operator
    if (length(conditions) == 1) {
      filter_expr <- conditions[[1]]
    } else {
      filter_expr <- reduce(conditions, function(x, y) expr(!!x & !!y))
    }
    
    return(data %>% filter(!!filter_expr))
  }
  
  # Attempt coalescing if fallback column exists
  if (!is.null(fallback_date_col)) {
    coalesce_attempt <- tryCatch(
      {
        result <- cohort_data %>%
          mutate(
            !!sym(coalesced_date_col_name) := coalesce(
              !!sym(primary_date_col),
              !!sym(fallback_date_col)
            )
          )
        
        # Apply date filters
        result <- create_date_filter(result, coalesced_date_col_name)
        
        cols <- c(
          "patid",
          "encounterid",
          event_code_col,
          coalesced_date_col_name
        )
        
        if (!is.null(enc_type_fil)) {
          cols <- c(cols, "enc_type")
        }
        
        result <- result %>%
          distinct(across(all_of(cols))) %>%
          rename(
            !!sym(criterion_encounterid) := encounterid,
            !!sym(event_code_col_criterion_suffix) := !!sym(event_code_col),
            !!sym(criterion_enc_type) := any_of("enc_type")
          )
        
        #'  result <- result %>%
        #'    distinct(
        #'      patid,
        #'      encounterid,
        #'      any_of("enc_type"),
        #'      !!sym(event_code_col),
        #'      !!sym(coalesced_date_col_name),
        #'    ) %>%
        #'    rename(
        #'      !!sym(criterion_encounterid) := encounterid,
        #'      !!sym(event_code_col_criterion_suffix) := !!sym(event_code_col),
        #'      !!sym(criterion_enc_type) := any_of("enc_type")
        #'    ) %>%
        #'    compute_new(., indexes = list("patid"))
        
        result
      },
      error = function(e) {
        warning(sprintf(
          "Coalescing %s and %s failed: %s. Falling back to primary date column only.",
          primary_date_col,
          fallback_date_col,
          e$message
        ))
        NULL
      }
    )
    
    # Check if coalescing was successful and returned patients
    if (
      !is.null(coalesce_attempt) &&
      distinct_ct(coalesce_attempt, id_col = "patid") > 0
    ) {
      echo_text(sprintf(
        "Successfully coalesced %s and %s",
        primary_date_col,
        fallback_date_col
      ))
      return(coalesce_attempt)
    } else {
      echo_text(sprintf(
        "Coalescing %s and %s yielded no patients, using primary date column only",
        primary_date_col,
        fallback_date_col
      ))
    }
  }
  
  # Use primary date column only
  result <- cohort_data %>%
    mutate(!!sym(coalesced_date_col_name) := !!sym(primary_date_col))
  
  # Apply date filters
  result <- create_date_filter(result, coalesced_date_col_name)
  
  cols <- c(
    "patid",
    "encounterid",
    event_code_col,
    coalesced_date_col_name
  )
  
  if (!is.null(enc_type_fil)) {
    cols <- c(cols, "enc_type")
  }
  
  result <- result %>%
    distinct(across(all_of(cols))) %>%
    rename(
      !!sym(criterion_encounterid) := encounterid,
      !!sym(criterion_enc_type) := any_of("enc_type"),
      !!sym(event_code_col_criterion_suffix) := !!sym(event_code_col)
    ) %>%
    compute_new(., indexes = list("patid"))
  
  return(result)
}

#' Create summary of distinct dates by patient
obtain_first_last_events <- function(
    cohort_data,
    date_col,
    min_codes_required
) {
  first_col_name <- paste0("first_", date_col)
  last_col_name <- paste0("last_", date_col)
  
  result <- cohort_data %>%
    group_by(patid) %>%
    summarize(
      distinct_dates = n_distinct(!!sym(date_col)),
      !!sym(first_col_name) := min(!!sym(date_col), na.rm = TRUE),
      !!sym(last_col_name) := max(!!sym(date_col), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(distinct_dates >= min_codes_required) %>%
    compute_new(., indexes = list("patid"))
  
  return(result)
}

#' Apply days separation logic with support for random selection
apply_days_separation <- function(
    summary_data,
    cohort_data,
    table_config,
    date_col,
    start_date,
    end_date,
    min_days_separation,
    qualifying_event,
    encounterid_criterion,
    event_code_criterion,
    enc_type_criterion
) {
  first_col_name <- paste0("first_", date_col)
  last_col_name <- paste0("last_", date_col)
  
  start_val <- resolve_date_input(start_date)
  end_val <- resolve_date_input(end_date)
  
  # Join and calculate days separation
  summarized <- summary_data %>%
    inner_join(cohort_data, by = "patid") %>%
    mutate(
      days_sep_from_first = sql(calc_days_between_dates(
        first_col_name,
        date_col
      )),
      days_sep_from_last = sql(calc_days_between_dates(date_col, last_col_name))
    ) %>%
    # Handle edge case for single event
    mutate(
      days_sep_from_first = case_when(
        days_sep_from_first == 0 & days_sep_from_last > 0 ~ 1,
        TRUE ~ days_sep_from_first
      ),
      days_sep_from_last = case_when(
        days_sep_from_last == 0 & days_sep_from_first > 0 ~ 1,
        TRUE ~ days_sep_from_last
      )
    ) %>%
    filter(
      days_sep_from_first >= min_days_separation &
        days_sep_from_last >= min_days_separation
    )
  
  # Apply qualifying event selection
  
  # Select final columns
  result <- filter_events(
    summarized,
    qualifying_event = qualifying_event,
    date_col = date_col
  ) %>%
    select(
      patid,
      !!sym(encounterid_criterion),
      matches(enc_type_criterion),
      !!sym(date_col),
      !!sym(event_code_criterion)
    ) %>%
    compute_new(., indexes = list("patid"))
  
  return(result)
}


#'  @export
filter_events <- function(summarized, qualifying_event, date_col) {
  if (qualifying_event == "first") {
    result <- summarized %>%
      group_by(patid) %>%
      slice_min(!!sym(date_col), with_ties = FALSE) %>%
      ungroup()
  } else if (qualifying_event == "last") {
    result <- summarized %>%
      group_by(patid) %>%
      slice_max(!!sym(date_col), with_ties = FALSE) %>%
      ungroup()
  } else if (qualifying_event == "random") {
    result <- summarized %>%
      group_by(patid) %>%
      slice_sample(n = 1) %>%
      ungroup()
  } else if (qualifying_event == "all") {
    result <- summarized
  } else {
    stop(sprintf("Unsupported qualifying_event: %s", qualifying_event))
  }
}


#' Validate final cohort with enhanced messaging
validate_final_cohort <- function(final_cohort, table_name) {
  patient_count <- distinct_ct(final_cohort, id_col = "patid")
  
  if (patient_count == 0) {
    warning(sprintf(
      "No patients qualified the cohort definition for table '%s'. This may result in empty results.
       If you expect patients at your datamart, please verify your criteria and report to qf@pcornet.org",
      table_name
    ))
  } else {
    message(sprintf(
      "Cohort development for table '%s' returned non-zero patient(s). Further computations can continue.",
      table_name
    ))
  }
  
  return(final_cohort)
}

# Generic S3 method that handles most table types
#' Generic method for define_criteria using table configuration
#' @export
define_criteria.generic <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  # Get table configuration
  table_config <- get_table_config(codeset)
  table_name <- table_config$table
  
  message(sprintf("Processing %s table using generic method...", table_name))
  
  # Get input table with optional cohort filtering
  if (!is.null(cohort)) {
    input_tbl <- cdm_tbl(table_name) %>%
      inner_join(cohort, by = "patid")
  } else {
    input_tbl <- cdm_tbl(table_name)
  }
  
  # Step 1: Filter for codes of interest
  if (.GlobalEnv$cdm_type == "pcornet") {
    # Check if there is a wild card request
    if (all(pull(codeset, code) == "*")) {
      echo_text('Wild card request detected')
      
      codetype_value <- codeset %>% pull(codetype)
      
      type_value <- codesets$crosswalk %>%
        filter(codetype %in% codetype_value) %>%
        pull()
      
      cohort_with_codes <- input_tbl %>%
        filter(!!sym(table_config$type_column) %in% type_value)
    } else {
      # Joining by code to `table_config$code_column`
      cohort_with_codes <- input_tbl %>%
        inner_join(codeset, by = setNames("code", table_config$code_column))
    }
  } else {
    cohort_with_codes <- input_tbl %>%
      inner_join(codeset, by = setNames("concept_id", table_config$code_column))
  }
  
  if (.GlobalEnv$cdm_type == "pcornet") {
    if (!is.null(enc_type_fil)) {
      if (table_name %in% c('diagnosis', 'procedure')) {
        cohort_with_codes <- cohort_with_codes %>%
          filter(enc_type %in% enc_type_fil)
      } else {
        cohort_with_codes <- cohort_with_codes %>%
          inner_join(
            cdm_tbl('encounter') %>% select(patid, encounterid, enc_type)
          ) %>%
          filter(enc_type %in% enc_type_fil)
      }
    }
  } else {}
  
  echo_text(sprintf(
    "Filtered for at least one %s code of interest",
    table_config$code_column
  ))
  
  # Step 2: Apply date filters
  cohort_in_query_period <- apply_date_filters(
    cohort_data = cohort_with_codes,
    table_config = table_config,
    start_date = start_date,
    end_date = end_date,
    criterion_suffix = criterion_suffix,
    enc_type_fil = enc_type_fil
  )
  
  echo_text("Filtered patients within query period")
  
  date_col_name <- paste0("criterion_", criterion_suffix, "_date")
  
  if (min_codes_required == 1) {
    final_cohort <- filter_events(
      cohort_in_query_period,
      qualifying_event = qualifying_event,
      date_col = date_col_name
    ) %>%
      compute_new(
        name = glue("{table_name}_{criterion_suffix}"),
        indexes = list('patid')
      )
    
    return(validate_final_cohort(final_cohort, table_name))
  } else {
    # Step 3: Apply minimum codes requirement
    
    distinct_events_summary <- obtain_first_last_events(
      cohort_data = cohort_in_query_period,
      date_col = date_col_name,
      min_codes_required = min_codes_required
    )
    
    echo_text("Applied minimum codes requirement")
    
    # Step 4: Apply days separation requirement
    encounterid_criterion <- paste0("encounterid_", criterion_suffix)
    event_code_criterion <- paste0(
      table_config$code_column,
      '_',
      criterion_suffix
    )
    enc_type_criterion <- paste0("enc_type_", criterion_suffix)
    
    final_cohort <- apply_days_separation(
      summary_data = distinct_events_summary,
      cohort_data = cohort_in_query_period,
      table_config = table_config,
      date_col = date_col_name,
      start_date = start_date,
      end_date = end_date,
      min_days_separation = min_days_separation,
      qualifying_event = qualifying_event,
      encounterid_criterion = encounterid_criterion,
      event_code_criterion = event_code_criterion,
      enc_type_criterion = enc_type_criterion
    )
    
    # Step 5: Validate and return
    return(validate_final_cohort(final_cohort, table_name))
  }
}

# Specific S3 methods that delegate to generic (with option for customization)

#' @export
define_criteria.diagnosis <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

# Specific S3 methods that delegate to generic (with option for customization)

#' @export
define_criteria.condition_occurrence <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}


#' @export
define_criteria.procedures <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.medication <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  # Get table configuration
  med_table_config <- get_table_config(codeset)
  med_tables <- list()
  # Identify which medication tables to hit based on codetype
  med_codetypes <- unique(codeset %>% select(codetype) %>% pull(codetype))
  
  for (table in names(med_table_config)) {
    if (table %in% c("primary_date_column", "code_column")) {
      next
    }
    if (any(med_codetypes %in% med_table_config[[table]]$permitted_codetype)) {
      med_tables[[table]] <- table
    }
  }
  
  # Get input table with optional cohort filtering
  combined_meds <- list()
  cohort_with_codes_list <- list()
  for (table_name in names(med_tables)) {
    message(sprintf("Processing %s table \n", table_name))
    
    table_config <- med_table_config[[table_name]]
    
    if (!is.null(cohort)) {
      input_tbl <- cdm_tbl(table_name) %>%
        inner_join(cohort, by = "patid")
    } else {
      input_tbl <- cdm_tbl(table_name)
    }
    # Joining by code to `table_config$code_column`
    cohort_with_codes_list[[table_name]] <- input_tbl %>%
      inner_join(
        codeset %>%
          filter(codetype %in% table_config$permitted_codetype),
        by = setNames("code", table_config$code_column)
      ) %>%
      select(
        patid,
        matches('encounterid'),
        matches('enc_type'),
        medication_code = !!sym(table_config$code_column),
        medication_date = table_config$primary_date_column
      ) %>%
      compute_new(
        name = paste0(table_name, '_', criterion_suffix),
        indexes = list('patid')
      )
  }
  
  cohort_with_codes <- reduce(cohort_with_codes_list, union_all) %>%
    compute_new(
      name = glue("medications_{criterion_suffix}"),
      indexes = list('patid')
    )
  
  for (table_name in names(med_tables)) {
    if (db_exists_table(name = glue("{table_name}_{criterion_suffix}"))) {
      db_remove_table(name = glue("{table_name}_{criterion_suffix}"))
      echo_text(glue("removed intermediate {table_name}_{criterion_suffix}"))
    }
  }
  
  if (.GlobalEnv$cdm_type == "pcornet") {
    if (!is.null(enc_type_fil)) {
      cohort_with_codes <- cohort_with_codes %>%
        inner_join(
          cdm_tbl('encounter') %>% select(patid, encounterid, enc_type)
        ) %>%
        filter(enc_type %in% enc_type_fil)
    }
  } else {}
  
  # Step 2: Apply date filters
  cohort_in_query_period <- apply_date_filters(
    cohort_data = cohort_with_codes,
    table_config = med_table_config,
    start_date = start_date,
    end_date = end_date,
    criterion_suffix = criterion_suffix,
    enc_type_fil = enc_type_fil
  )
  
  echo_text("Filtered patients within query period")
  date_col_name <- paste0("criterion_", criterion_suffix, "_date")
  
  if (min_codes_required == 1) {
    final_cohort <- filter_events(
      cohort_in_query_period,
      qualifying_event = qualifying_event,
      date_col = date_col_name
    ) %>%
      compute_new(., indexes = list('patid'))
    
    if (db_exists_table(name = glue("medications_{criterion_suffix}"))) {
      db_remove_table(name = glue("medications_{criterion_suffix}"))
      echo_text(glue("removed intermediate medications_{criterion_suffix}"))
    }
    
    return(validate_final_cohort(final_cohort, table_name))
  } else {
    # Step 3: Apply minimum codes requirement
    distinct_events_summary <- obtain_first_last_events(
      cohort_data = cohort_in_query_period,
      date_col = date_col_name,
      min_codes_required = min_codes_required
    )
    
    echo_text("Applied minimum codes requirement")
    
    # Step 4: Apply days separation requirement
    encounterid_criterion <- paste0("encounterid_", criterion_suffix)
    medication_criterion <- paste0("medication_code_", criterion_suffix)
    enc_type_criterion <- paste0("enc_type_", criterion_suffix)
    
    final_cohort <- apply_days_separation(
      summary_data = distinct_events_summary,
      cohort_data = cohort_in_query_period,
      table_config = table_config,
      date_col = date_col_name,
      start_date = start_date,
      end_date = end_date,
      min_days_separation = min_days_separation,
      qualifying_event = qualifying_event,
      encounterid_criterion = encounterid_criterion,
      event_code_criterion = medication_criterion,
      enc_type_criterion = enc_type_criterion
    )
    
    # Step 5: Validate and return
    return(validate_final_cohort(final_cohort, table_name))
  }
}

#' @export
define_criteria.dispensing <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.prescribing <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.med_admin <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.drug_exposure <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.obs_clin <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.immunization <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' @export
define_criteria.death <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix,
    enc_type_fil = NULL
) {
  define_criteria.generic(
    cohort,
    codeset,
    start_date,
    end_date,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil
  )
}

#' # Example of a specialized method that might need custom logic
#' #' Lab results may need special handling for lab values, ranges, etc.
#' #' @export
#' define_criteria.lab_result_cm <- function(
    #' 		cohort = NULL,
#' 		codeset,
#' 		start_date = NULL,
#' 		end_date = NULL,
#' 		min_codes_required = 1,
#' 		min_days_separation = 0,
#' 		qualifying_event = "first",
#' 		criterion_suffix,
#' 		lab_value_filter = NULL
#' ) {
#' 	# Start with generic processing
#' 	result <- define_criteria.generic(
#' 		cohort,
#' 		codeset,
#' 		start_date,
#' 		end_date,
#' 		min_codes_required,
#' 		min_days_separation,
#' 		qualifying_event,
#' 		criterion_suffix
#' 	)
#'
#' 	# Add custom lab-specific logic here if needed
#' 	if (!is.null(lab_value_filter)) {
#' 		message("Applying lab value filters...")
#' 		# Custom lab filtering logic would go here
#' 	}
#'
#' 	return(result)
#' }

#' Default method with enhanced error messaging
#' @export
define_criteria.default <- function(
    cohort = NULL,
    codeset,
    start_date = NULL,
    end_date = NULL,
    min_codes_required,
    min_days_separation,
    qualifying_event,
    criterion_suffix,
    enc_type_fil = NULL
) {
  # Get available codetypes from the codeset
  available_codetypes <- unique(codeset %>% distinct(codetype) %>% pull())
  
  cli_abort(
    c(
      "✗" = "Error: Unknown or unsupported codetype value(s)",
      "i" = sprintf(
        "Found codetype(s): %s",
        paste(available_codetypes, collapse = ", ")
      ),
      "i" = "Supported codetypes by table:",
      "i" = "diagnosis: DX09, DX10, DX11, DXSM",
      "i" = "procedure: PX09, PX10, PX11, PXCH, PXLC, PXND, PXRE",
      "i" = "dispensing: RX01, RX11, RX09",
      "i" = "prescribing: PR00",
      "i" = "lab_result_cm: LBLC, LBCH, LB09, LB10, LB11",
      "i" = "med_admin: MA09, MA11, MA00",
      "i" = "obs_clin: OCSM, OCLC",
      "i" = "immunization: VXCX, VXND, VXCH, VXRX",
      "i" = "death: DTH"
    )
  )
}