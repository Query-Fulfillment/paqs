# Constants and Configuration
COLUMN_NAMES <- list(
  PATIENT_ID = "patid",
  ENCOUNTER_ID = "encounterid", 
  DIAGNOSIS_CODE = "dx",
  DIAGNOSIS_DATE = "dx_date",
  PROCEDURE_CODE = "px",
  PROCEDURE_DATE = "px_date",
  ADMIT_DATE = "admit_date",
  DISPENSE_DATE = "dispense_date",
  RX_ORDER_DATE = "rx_order_date",
  LAB_ORDER_DATE = "lab_order_date",
  RESULT_DATE = "result_date"
)

# Table configurations for different code types
TABLE_CONFIGS <- list(
  diagnosis = list(
    table = "diagnosis",
    code_column = "dx",
    primary_date_column = "dx_date",
    fallback_date_column = "admit_date"
  ),
  procedure = list(
    table = "procedures", 
    code_column = "px",
    primary_date_column = "px_date",
    fallback_date_column = "admit_date"
  ),
  dispensing = list(
    table = "dispensing",
    code_column = "ndc",
    primary_date_column = "dispense_date",
    fallback_date_column = NULL
  ),
  prescribing = list(
    table = "prescribing",
    code_column = "rxnorm_cui",
    primary_date_column = "rx_order_date", 
    fallback_date_column = NULL
  ),
  lab_result_cm = list(
    table = "lab_result_cm",
    code_column = "lab_loinc",
    primary_date_column = "result_date",
    fallback_date_column = "lab_order_date"
  ),
  med_admin = list(
    table = "med_admin",
    code_column = "medadmin_code",
    primary_date_column = "medadmin_start_date",
    fallback_date_column = NULL
  ),
  obs_clin = list(
    table = "obs_clin",
    code_column = "obsclin_code", 
    primary_date_column = "obsclin_date",
    fallback_date_column = NULL
  ),
  immunization = list(
    table = "immunization",
    code_column = "vx_code",
    primary_date_column = "vx_admin_date",
    fallback_date_column = NULL
  ),
  death = list(
    table = "death",
    code_column = "death_cause_code",
    primary_date_column = "death_date", 
    fallback_date_column = NULL
  )
)

# Input validation functions
validate_all_inputs <- function(codeset, start_date, end_date, min_codes_required, 
                               min_days_separation, qualifying_event, criterion_suffix) {
  
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
  if (!is.numeric(min_codes_required) || min_codes_required < 1 || min_codes_required != as.integer(min_codes_required)) {
    stop("min_codes_required must be a positive integer >= 1")
  }
  
  if (!is.numeric(min_days_separation) || min_days_separation < 0 || min_days_separation != as.integer(min_days_separation)) {
    stop("min_days_separation must be a non-negative integer >= 0")
  }
  
  # Validate qualifying_event
  if (!qualifying_event %in% c("first", "last", "random")) {
    stop("qualifying_event must be one of: 'first', 'last', 'random'")
  }
  
  # Validate criterion_suffix
  if (!is.character(criterion_suffix) || length(criterion_suffix) != 1 || nchar(criterion_suffix) == 0) {
    stop("criterion_suffix must be a non-empty character string")
  }
  
  # Validate date inputs (basic check - more detailed validation in resolve_date_input)
  tryCatch({
    resolve_date_input(start_date)
    resolve_date_input(end_date)
  }, error = function(e) {
    stop(paste("Invalid date format:", e$message))
  })
}

validate_date_range <- function(start_date, end_date) {
  start_val <- resolve_date_input(start_date)
  end_val <- resolve_date_input(end_date)
  
  # Only validate if both are actual dates (not column references)
  if (inherits(start_val, "Date") && inherits(end_val, "Date")) {
    if (start_val >= end_val) {
      stop("start_date must be before end_date")
    }
  }
}

# Enhanced date resolution function
resolve_date_input <- function(x) {
  if (inherits(x, "Date")) {
    return(x)
  } else if (is.character(x)) {
    # Try parsing using multiple common formats
    parsed <- suppressWarnings(parse_date_time(x, orders = c(
      "Ymd", "mdY", "dmY", "Y-m-d", "m/d/Y", "d/m/Y",
      "Y/m/d", "Y.m.d", "B d, Y", "d B Y"
    )))
    if (!is.na(parsed)) {
      return(as.Date(parsed))
    } else {
      return(sym(x))  # Must be a column name
    }
  } else if (is_symbol(x)) {
    return(x)
  } else {
    stop("Date input must be either a Date object, date string, or column name")
  }
}

# Enhanced codetype to table mapping
match_codetype_to_table <- function() {
  tibble::tribble(~codetype, ~table,
    # diagnosis
    'DX09', 'diagnosis',
    'DX10', 'diagnosis', 
    'DX11', 'diagnosis',
    'DXSM', 'diagnosis',
    # dispensing
    'RX01', 'dispensing',
    'RX11', 'dispensing', 
    'RX09', 'dispensing',
    # procedure
    'PX09', 'procedures',
    'PX10', 'procedures',
    'PX11', 'procedures', 
    'PXCH', 'procedures',
    'PXLC', 'procedures',
    'PXND', 'procedures',
    'PXRE', 'procedures',
    # prescribing
    'PR00', 'prescribing',
    # lab_result_cm
    'LBLC', 'lab_result_cm',
    'LBCH', 'lab_result_cm',
    'LB09', 'lab_result_cm',
    'LB10', 'lab_result_cm', 
    'LB11', 'lab_result_cm',
    # med_admin
    'MA09', 'med_admin',
    'MA11', 'med_admin',
    'MA00', 'med_admin',
    # obs_clin
    'OCSM', 'obs_clin',
    'OCLC', 'obs_clin',
    # immunization
    'VXCX', 'immunization',
    'VXND', 'immunization',
    'VXCH', 'immunization', 
    'VXRX', 'immunization',
    # death
    'DTH', 'death'
  ) %>%
    copy_to_new(df = ., name = "crosswalk", overwrite = TRUE, temporary = TRUE)
}

get_table_config <- function(codeset) {
  codetype_mapping <- match_codetype_to_table() %>%
    inner_join(codeset, by = "codetype") %>%
    distinct(table) %>%
    pull()
  
  if (length(codetype_mapping) == 0) {
    stop("No valid codetype found in codeset")
  }
  
  if (length(codetype_mapping) > 1) {
    warning("Multiple codetypes found in codeset. Using the first one for method dispatch.")
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
#'
#' @return A tibble with patid, encounterid, and criterion date
#' @export
define_criteria <- function(cohort = NULL, codeset, start_date, end_date, 
                          min_codes_required = 1, min_days_separation = 0, 
                          qualifying_event = "first", criterion_suffix) {
  
  # Comprehensive input validation
  validate_all_inputs(codeset, start_date, end_date, min_codes_required, 
                     min_days_separation, qualifying_event, criterion_suffix)
  
  validate_date_range(start_date, end_date)
  
  # Get table configuration for dispatch
  table_config <- get_table_config(codeset)
  table_name <- table_config$table
  
  # Set class for S3 dispatch
  class(codeset) <- c(table_name, class(codeset))
  
  # Log automatic adjustment for single code requirement
  if (min_codes_required == 1) {
    message("min_codes_required is set to 1, automatically setting min_days_separation to 0")
    min_days_separation <- 0
  }
  
  # Use S3 method dispatch
  UseMethod("define_criteria", codeset)
}

# Enhanced helper functions

#' Apply date filters with robust error handling
apply_date_filters <- function(cohort_data, table_config, start_date, end_date, criterion_suffix) {
  
  primary_date_col <- table_config$primary_date_column
  fallback_date_col <- table_config$fallback_date_column
  event_code_col <- table_config$code_column
  
  coalesced_date_col_name <- paste0("criterion_", criterion_suffix, "_date")
  criterion_encounterid <- paste0("encounterid_", criterion_suffix)
  
  start_val <- resolve_date_input(start_date)
  end_val <- resolve_date_input(end_date)
  
  # Attempt coalescing if fallback column exists
  if (!is.null(fallback_date_col)) {
    coalesce_attempt <- tryCatch({
      cohort_data %>%
        mutate(!!sym(coalesced_date_col_name) := coalesce(!!sym(primary_date_col), !!sym(fallback_date_col))) %>%
        filter(!!sym(coalesced_date_col_name) >= !!start_val & !!sym(coalesced_date_col_name) <= !!end_val) %>%
        distinct(patid, encounterid, !!sym(event_code_col), !!sym(coalesced_date_col_name)) %>%
        rename(!!sym(criterion_encounterid) := encounterid) %>%
        compute_new(., indexes = list("patid"))
    }, error = function(e) {
      warning(sprintf("Coalescing %s and %s failed: %s. Falling back to primary date column only.", 
                     primary_date_col, fallback_date_col, e$message))
      NULL
    })
    
    # Check if coalescing was successful and returned patients
    if (!is.null(coalesce_attempt) && distinct_ct(coalesce_attempt, id_col = "patid") > 0) {
      echo_text(sprintf("Successfully coalesced %s and %s", primary_date_col, fallback_date_col))
      return(coalesce_attempt)
    } else {
      echo_text(sprintf("Coalescing %s and %s yielded no patients, using primary date column only", 
                       primary_date_col, fallback_date_col))
    }
  }
  
  # Use primary date column only
  result <- cohort_data %>%
    mutate(!!sym(coalesced_date_col_name) := !!sym(primary_date_col)) %>%
    filter(!!sym(coalesced_date_col_name) >= !!start_val & !!sym(coalesced_date_col_name) <= !!end_val) %>%
    distinct(patid, encounterid, !!sym(event_code_col), !!sym(coalesced_date_col_name)) %>%
    rename(!!sym(criterion_encounterid) := encounterid) %>%
    compute_new(., indexes = list("patid"))
  
  return(result)
}

#' Create summary of distinct dates by patient
obtain_first_last_events <- function(cohort_data, date_col, min_codes_required) {
  
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
apply_days_separation <- function(summary_data, cohort_data, date_col, start_date, end_date, 
                                min_days_separation, qualifying_event, encounterid_criterion) {
  
  first_col_name <- paste0("first_", date_col)
  last_col_name <- paste0("last_", date_col)
  
  start_val <- resolve_date_input(start_date)
  end_val <- resolve_date_input(end_date)
  
  # Join and calculate days separation
  summarized <- summary_data %>%
    inner_join(cohort_data, by = "patid") %>%
    mutate(
      days_sep_from_first = sql(calc_days_between_dates(first_col_name, date_col)),
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
    filter(days_sep_from_first >= min_days_separation & days_sep_from_last >= min_days_separation)
  
  # Apply qualifying event selection
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
  } else {
    stop(sprintf("Unsupported qualifying_event: %s", qualifying_event))
  }
  
  # Select final columns
  result <- result %>%
    select(patid, !!sym(encounterid_criterion), !!sym(date_col)) %>%
    compute_new(., indexes = list("patid"))
  
  return(result)
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
      "Cohort development for table '%s' returned %d patient(s). Further computations can continue.",
      table_name, patient_count
    ))
  }
  
  return(final_cohort)
}

# Generic S3 method that handles most table types
#' Generic method for define_criteria using table configuration
#' @export
define_criteria.generic <- function(cohort = NULL, codeset, start_date, end_date, 
                                  min_codes_required = 1, min_days_separation = 0, 
                                  qualifying_event = "first", criterion_suffix) {
  
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
  cohort_with_codes <- input_tbl %>%
    inner_join(codeset, by = setNames("code", table_config$code_column))
  
  echo_text(sprintf("Filtered for at least one %s code of interest", table_config$code_column))
  
  # Step 2: Apply date filters
  cohort_in_query_period <- apply_date_filters(
    cohort_data = cohort_with_codes,
    table_config = table_config,
    start_date = start_date,
    end_date = end_date,
    criterion_suffix = criterion_suffix
  )
  
  echo_text("Filtered patients within query period")
  
  # Step 3: Apply minimum codes requirement
  date_col_name <- paste0("criterion_", criterion_suffix, "_date")
  distinct_events_summary <- obtain_first_last_events(
    cohort_data = cohort_in_query_period,
    date_col = date_col_name,
    min_codes_required = min_codes_required
  )
  
  echo_text("Applied minimum codes requirement")
  
  # Step 4: Apply days separation requirement
  encounterid_criterion <- paste0("encounterid_", criterion_suffix)
  final_cohort <- apply_days_separation(
    summary_data = distinct_events_summary,
    cohort_data = cohort_in_query_period,
    date_col = date_col_name,
    start_date = start_date,
    end_date = end_date,
    min_days_separation = min_days_separation,
    qualifying_event = qualifying_event,
    encounterid_criterion = encounterid_criterion
  )
  
  # Step 5: Validate and return
  return(validate_final_cohort(final_cohort, table_name))
}

# Specific S3 methods that delegate to generic (with option for customization)

#' @export
define_criteria.diagnosis <- function(cohort = NULL, codeset, start_date, end_date, 
                                    min_codes_required = 1, min_days_separation = 0, 
                                    qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date, 
                         min_codes_required, min_days_separation, 
                         qualifying_event, criterion_suffix)
}

#' @export  
define_criteria.procedures <- function(cohort = NULL, codeset, start_date, end_date,
                                    min_codes_required = 1, min_days_separation = 0,
                                    qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation, 
                         qualifying_event, criterion_suffix)
}

#' @export
define_criteria.dispensing <- function(cohort = NULL, codeset, start_date, end_date,
                                     min_codes_required = 1, min_days_separation = 0,
                                     qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation,
                         qualifying_event, criterion_suffix)
}

#' @export
define_criteria.prescribing <- function(cohort = NULL, codeset, start_date, end_date,
                                      min_codes_required = 1, min_days_separation = 0,
                                      qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation,
                         qualifying_event, criterion_suffix)
}

#' @export
define_criteria.med_admin <- function(cohort = NULL, codeset, start_date, end_date,
                                    min_codes_required = 1, min_days_separation = 0,
                                    qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation,
                         qualifying_event, criterion_suffix)
}

#' @export
define_criteria.obs_clin <- function(cohort = NULL, codeset, start_date, end_date,
                                   min_codes_required = 1, min_days_separation = 0,
                                   qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation,
                         qualifying_event, criterion_suffix)
}

#' @export
define_criteria.immunization <- function(cohort = NULL, codeset, start_date, end_date,
                                       min_codes_required = 1, min_days_separation = 0,
                                       qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation,
                         qualifying_event, criterion_suffix)
}

#' @export
define_criteria.death <- function(cohort = NULL, codeset, start_date, end_date,
                                min_codes_required = 1, min_days_separation = 0,
                                qualifying_event = "first", criterion_suffix) {
  define_criteria.generic(cohort, codeset, start_date, end_date,
                         min_codes_required, min_days_separation,
                         qualifying_event, criterion_suffix)
}

# Example of a specialized method that might need custom logic
#' Lab results may need special handling for lab values, ranges, etc.
#' @export
define_criteria.lab_result_cm <- function(cohort = NULL, codeset, start_date, end_date,
                                        min_codes_required = 1, min_days_separation = 0,
                                        qualifying_event = "first", criterion_suffix,
                                        lab_value_filter = NULL, normal_range_only = FALSE) {
  
  # Start with generic processing
  result <- define_criteria.generic(cohort, codeset, start_date, end_date,
                                   min_codes_required, min_days_separation,
                                   qualifying_event, criterion_suffix)
  
  # Add custom lab-specific logic here if needed
  if (!is.null(lab_value_filter)) {
    message("Applying lab value filters...")
    # Custom lab filtering logic would go here
  }
  
  return(result)
}

#' Default method with enhanced error messaging
#' @export
define_criteria.default <- function(cohort = NULL,codeset, start_date, end_date, min_codes_required, 
                                  min_days_separation, qualifying_event, criterion_suffix) {
  
  # Get available codetypes from the codeset
  available_codetypes <- unique(codeset %>% distinct(codetype))
  
  cli_abort(
    c("✗" = "Error: Unknown or unsupported codetype value(s)",
      "i" = sprintf("Found codetype(s): %s", paste(available_codetypes, collapse = ", ")),
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