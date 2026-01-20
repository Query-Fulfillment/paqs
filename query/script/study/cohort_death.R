#' Capture death record
#' add in a parameter in define_criteria - to filter for alive/deceased patients

#' @export
define_criteria.death <- function(
    cohort = NULL,
    codeset = NULL,
    start_date,
    end_date,
    min_codes_required = 1,
    min_days_separation = 0,
    qualifying_event = "first",
    criterion_suffix
) {
  echo_text("Starting define_criteria.death...")
  
  if (!is.null(cohort)) {
    echo_text("Joining death table with supplied cohort")
    input_tbl <- cdm_tbl("death") %>%
      inner_join(cohort, by = "patid") %>%
      compute_new(indexes = list("patid"))
  } else {
    echo_text("No cohort supplied, using full death table")
    input_tbl <- cdm_tbl("death")
  }
  
  
  echo_text("Selecting patid and death_date columns only")
  input_tbl <- input_tbl %>%
    select(patid, death_date) %>%
    compute_new(indexes = list("patid"))
  
  
  
  
  start_date_resolved <- resolve_date_input(start_date)
  end_date_resolved   <- resolve_date_input(end_date)
  
  
  result <- input_tbl %>%
    filter(
      death_date >= start_date_resolved &
        death_date <= end_date_resolved
    ) %>%
    rename(!!paste0("criterion_", criterion_suffix, "_date") := death_date) %>%
    #' SM Edits
    #' Removing .keep_all as Oracle requies to order by in the sql statement to use .keep_all. Instead using select + distinct
    select(patid,!!sym(paste0("criterion_", criterion_suffix, "_date"))) %>%
    distinct() %>%
    compute_new(indexes = list("patid"))
  
  
  return(validate_final_cohort(result, "death"))
}