get_demographic <- function(
    cohort = NULL,
    end_date = NULL,
    ce_date = NULL
) {
  
  echo_text("Starting get_demographic...")
  table_name <- "demographic"
  
  input_tbl <- if (!is.null(cohort)) {
    echo_text("Joining demographic table with supplied cohort")
    cdm_tbl(table_name) %>% inner_join(cohort, by = "patid")%>%
      compute_new(indexes = list("patid"))
  } else {
    echo_text("No cohort supplied, using full demographic table")
    cdm_tbl(table_name)
  }
  
  # resolve_date_input
  end_date_resolved <- resolve_date_input(end_date)
  
  input_tbl_labels <- input_tbl %>%
    mutate(
      sex_label = dplyr::case_when(
        sex == "A"  ~ "Ambiguous",
        sex == "F"  ~ "Female",
        sex == "M"  ~ "Male",
        sex == "NI" ~ "No Information",
        sex == "UN" ~ "Unknown",
        sex == "OT" ~ "Other",
        TRUE ~ "Missing"
      ),
      hispanic_label = dplyr::case_when(
        hispanic == "Y"  ~ "Yes",
        hispanic == "N"  ~ "No",
        hispanic == "R"  ~ "Refuse to answer",
        hispanic == "NI" ~ "No Information",
        hispanic == "UN" ~ "Unknown",
        hispanic == "OT" ~ "Other",
        TRUE ~ "Missing"
      ),
      race_label = dplyr::case_when(
        race == "01" ~ "American Indian or Alaska Native",
        race == "02" ~ "Asian",
        race == "03" ~ "Black or African American",
        race == "04" ~ "Native Hawaiian or Other Pacific Islander",
        race == "05" ~ "White",
        race == "06" ~ "Multiple Race",
        race == "07" ~ "Refuse to answer",
        race == "NI" ~ "No Information",
        race == "UN" ~ "Unknown",
        race == "OT" ~ "Other",
        TRUE ~ "Missing"
      )
    ) %>%
    mutate(end_date_col = as.Date(end_date_resolved)) %>%
    compute_new(indexes = list("patid"))
  
  #	if (!is.null(end_date_col)) {
  
  
  input_tbl_final <- input_tbl_labels %>%
    mutate(
      age_at_end_days = sql(calc_days_between_dates("birth_date", "end_date_col")),
      age_at_end = as.integer(floor(age_at_end_days / 365.25)),
      age_cat = case_when(
        age_at_end >= 0  & age_at_end < 10  ~ "Children 0-9",
        age_at_end >= 10 & age_at_end < 20  ~ "Children 10-19",
        age_at_end >= 20 & age_at_end < 35  ~ "Adults 20-34",
        age_at_end >= 35 & age_at_end < 55  ~ "Adults 35-54",
        age_at_end >= 55 & age_at_end < 65  ~ "Adults 55-64",
        age_at_end >= 65 & age_at_end < 75  ~ "Adults 65-74",
        age_at_end >= 75 & age_at_end < 110 ~ "Adults 75+",
        age_at_end >= 110 ~ "Missing",
        TRUE ~ "Missing"
      ),
      year_of_hei  = as.character(lubridate::year(!!sym(ce_date))),
      month_of_hei = as.character(lubridate::month(!!sym(ce_date))),
      year_month_of_hei = paste0(year_of_hei, "-", month_of_hei)
    ) %>%
    compute_new(indexes = list("patid"))
  
  #	}
  
  demog_tbl <- input_tbl_final %>% select(patid, birth_date, age_at_end_days, age_at_end, age_cat, sex_label, race_label, hispanic_label, race, hispanic, sex,
                                          year_of_hei, month_of_hei, year_month_of_hei) %>%
    compute_new(indexes = list("patid"))
  
  
  return(validate_final_cohort(demog_tbl, table_name))
}