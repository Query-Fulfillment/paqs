#' Execute the request
#'
#' This function presumes the environment has been set up, and executes the
#' steps of the request.
#'
#' In addition to performing queries and analyses, the execution path in this
#' function should include periodic progress messages to the user, and logging
#' of intermediate totals and timing data through [append_sum()].
#'
#' @return The return value is dependent on the content of the request, but is
#'   typically a structure pointing to some or all of the retrieved data or
#'   analysis results.  The value is not used by the framework itself.
#' @md
run <- function() {
  # ===================================================================================================
  #' **Standard code DO NOT EDIT | Only fill values where necessary**
  # ===================================================================================================

  .GlobalEnv$total_steps <- 0L
  .GlobalEnv$test_stat <- init_sum(
    Test = "Start of Query",
    N = as.numeric(0),
    set_default = NULL
  )

  #' **SET YOUR QUERY NAME**
  .GlobalEnv$query_name <- "PAQS_TEST"

  start_log()

  # ===================================================================================================
  #' **IMPORTANT**
  #' **SET YOUR CONNECTION CDM TYPE | Permitted Values are `pcornet` or `omop`->**
  #'
  set_cdm_config('pcornet')

  # ===================================================================================================

  # ===================================================================================================
  #' **WRITE YOUR QUERY FROM HERE ->**
  # ===================================================================================================
  # Adding a list object to store results on the global environment ====

  rslt <- list()

  .GlobalEnv$codesets <- load_all_codesets()

  rslt$any_dx_2024 <- define_criteria(
    codeset = codesets$dx_any_icd,
    start_date = NULL,
    end_date = NULL,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix = "any_dx",
    enc_type_fil = c('AV', 'TH', 'ED', 'IP')
  )

  rslt$and_dx_any_glp <- define_criteria(
    cohort = rslt$any_dx_2024,
    codeset = codesets$rx_any_glp,
    start_date = "01-01-2013",
    end_date = "12-31-2024",
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix = "any_glp"
  )

  # ===================================================================================================
  #' **Standard code DO NOT EDIT**
  # ===================================================================================================

  end_log()

  render_report()
}
