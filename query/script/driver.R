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
  #' **Standard code DO NOT EDIT | Fill values where necessary**
  # ===================================================================================================

  init_message(query_title = "PAQS Query Development")


  .GlobalEnv$total_steps <- 0L
  .GlobalEnv$query_name <- "PAQS_DEV"

  .GlobalEnv$test_stat <- init_sum(Test = "Start of Query", Status = "", set_default = NULL)

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

  codesets <- load_all_codesets()
  
  # Diabetes
  
  # Atleast one occurrence of T2 Diabetes code between 2021 - 2024
  rslt$dx_t2_diabetes <- define_criteria(codeset = codesets$dx_t2_diabetes,
                                         start_date = "01-01-2015",
                                         end_date = "12-31-2024",
                                         min_codes_required = 1,
                                         min_days_separation = 0,
                                         qualifying_event = 'first',
                                         criterion_suffix = "dx_t2_diabetes") 
  
  # Atleast one occurrence of T2 Diabetes code between diagnosis date and 2024
  rslt$dx_t2_any_anti_diabetic_post_dx <- define_criteria(cohort = rslt$dx_t2_diabetes,
                                                  codeset = codesets$rx_any_anti_diabetic,
                                                  start_date = "criterion_dx_t2_diabetes_date",
                                                  end_date = "12-31-2024",
                                                  min_codes_required = 1,
                                                  min_days_separation = 0,
                                                  qualifying_event = 'first',
                                                  criterion_suffix = "any_anti_diabetic_post")
  
  rslt$dx_t2_any_anti_diabetic_pre_dx <- define_criteria(cohort = rslt$dx_t2_diabetes,
                                                  codeset = codesets$rx_any_anti_diabetic,
                                                  start_date = "01-01-2015",
                                                  end_date = "criterion_dx_t2_diabetes_date",
                                                  min_codes_required = 1,
                                                  min_days_separation = 0,
                                                  qualifying_event = 'first',
                                                  criterion_suffix = "any_anti_diabetic_pre")

  # Filtering patients who had an anti-diabetic medication exposure prior to their T2D diagnosis
  
  rslt$dx_t2_any_anti_diabetic_post_dx_no_prior <- rslt$dx_t2_any_anti_diabetic_post_dx %>% 
    anti_join(rslt$dx_t2_any_anti_diabetic_pre_dx, by = c('patid'))

   # Atleast one occurrence of T2 Diabetes code between diagnosis date and end of query period
  rslt$post_any_adb_any_glp <- define_criteria(cohort =   rslt$dx_t2_any_anti_diabetic_post_dx_no_prior,
                                               codeset = codesets$rx_any_glp,
                                               start_date = "criterion_any_anti_diabetic_post_date",
                                               end_date = "12-31-2024",
                                               min_codes_required = 1,
                                               min_days_separation = 0,
                                               qualifying_event = 'first',
                                               criterion_suffix = "any_glp")
   
  # Patients with a t2dm dx code and anti-diabetic medication + any glp 2 + bariatric surgery post dx date
    rslt$tdx_with_bariatric_surgery <- define_criteria(cohort = rslt$dx_t2_diabetes %>% inner_join(rslt$post_any_adb_any_glp %>% distinct(patid)),
                                                       codeset = codesets$px_bariatric_surgery,
                                                       start_date = "01-01-2015",
                                                       end_date = "12-31-2024",
                                                       min_codes_required = 1,
                                                       min_days_separation = 0,
                                                       qualifying_event = 'first',
                                                       criterion_suffix = "bariatric_surgery")

  # ===================================================================================================
  #' **Standard code DO NOT EDIT**
  # ===================================================================================================

  end_log()
  on.exit(exit())
  render_report()
}
