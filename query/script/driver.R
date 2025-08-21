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
  .GlobalEnv$test_stat <- init_sum(Test = "Start of Query", N = as.numeric(0), set_default = NULL)

  start_log()

  # ===================================================================================================
  #' **IMPORTANT**
  #' **SET YOUR CONNECTION CDM TYPE | Permitted Values are `pcornet` or `omop`->**
  #'
    set_cdm_config('pcornet')

  #' **SET YOUR QUERY NAME**
    .GlobalEnv$query_name <- ""

  # ===================================================================================================

  # ===================================================================================================
  #' **WRITE YOUR QUERY FROM HERE ->**
  # ===================================================================================================
  # Adding a list object to store results on the global environment ====

  rslt <- list()

  codesets <- load_all_codesets()




  # ===================================================================================================
  #' **Standard code DO NOT EDIT**
  # ===================================================================================================

  render_report()
  
  end_log()
  
  on.exit(exit())
}
