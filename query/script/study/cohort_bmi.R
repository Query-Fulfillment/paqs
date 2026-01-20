
#' Get BMI for cohort
#'
#' @param cohort cohort table with patid and index or anchor date for function to reference
#' @param vital_tbl vital cdm table
#' @param demographic_tbl demographic cdm table
#' @param cohort_date_col column name for index or anchor date for function to reference
#' @param adult_wt_days max number of days to look back for weight for adults (must be negative)
#' @param child_ht_days max number of days to look back for height for pediatrics (must be negative)
#' @param child_wt_days max number of days to look back for weight for pediatrics (must be negative)
#' @param infant_ht_days max number of days to look back for height for infants (must be negative)
#' @param infant_wt_days max number of days to look back for weight for infant (must be negative)
#'
get_bmi <- function(cohort,
                    vital_tbl = cdm_tbl('vital'),
                    demographic_tbl = cdm_tbl('demographic'),
                    cohort_date_col,
                    adult_wt_days = -365L, # has to be negative
                    adult_ht_days = -3650L, # has to be negative
                    child_ht_days = -90L, # has to be negative
                    child_wt_days = -90L, # has to be negative
                    infant_ht_days = -10L, # has to be negative
                    infant_wt_days = -10L # has to be negative
) {
  
  bmi_cat_rslt <- list()
  
  # This has to say 7305 days (20 years old) as the cutoff because CDC BMI child computation is for 2-19 year olds and anyone 20 and above is adult BMI
  child_age_cutoff <- 7305L
  
  echo_text("Step 1: Joining cohort table to demographic to find age")
  
  cohort_demo <- cohort %>%
    # join to demographic table to get birth_date so that we can calculate age
    inner_join(
      select(demographic_tbl, patid, birth_date, sex),
      by = "patid"
    ) %>%
    # rename cohort_date_col to index_date
    mutate(index_date = !!sym(cohort_date_col)) %>%
    # calculate the number of days old at index_date
    mutate(days_old_at_index = sql(calc_days_between_dates("birth_date", "index_date"))) %>%
    # make sure days_old_at_index is an integer
    mutate(days_old_at_index = as.integer(days_old_at_index)) %>% 
    # filter out anyone with a negative age, because we cannot compute BMI for them and there may be a data quality issue
    filter(days_old_at_index >= 0 ) %>%
    # filter out anyone with a missing age
    filter(!is.na(days_old_at_index)) %>%
    # categorize by age
    mutate(bmi_age = case_when(
      days_old_at_index >= 0 & days_old_at_index < 730 ~ "infant_less_than_2",
      days_old_at_index >= 730 & days_old_at_index < 7305 ~ "pediatric_2_to_19",
      days_old_at_index >= 7305 ~ "adult",
      TRUE ~ "missing"
    )) %>%
    # just in case anyone falls into this category, exclude them
    filter(bmi_age != "missing") %>%
    
    compute_new(indexes = list("patid"))
  
  
  # Join with vitals table -----------------------------------------------------
  echo_text("Step 2: Joining vitals table with supplied cohort")
  cohort_vitals <- cohort_demo %>%
    # inner join to vitals table
    inner_join(
      vital_tbl%>%
        select(patid, ht, wt, measure_date),
      by = "patid"
    ) %>%
    # make sure wt and ht are numeric
    mutate(wt = as.numeric(wt),
           ht = as.numeric(ht)) %>%
    compute_new(indexes = list("patid"))
  
  
  # Get infant, pediatric, and adult vitals tables --------------------------------------
  
  if (!cohort_vitals %>% count() %>% pull(n) == 0) {
    
    echo_text("Step 3: Get tables for infant, pediatric, and adult measurements")
    # Infant vitals
    infant_vitals_less_than_2 <-  cohort_vitals %>%
      filter(bmi_age == "infant_less_than_2") %>%
      compute_new(indexes = list("patid"))
    
    
    # Pediatric vitals
    child_vitals_2_to_19 <- cohort_vitals %>%
      filter(bmi_age == "pediatric_2_to_19") %>%
      compute_new(indexes = list("patid"))
    
    
    # Adult vitals
    adult_vitals <-	cohort_vitals %>%
      filter(bmi_age == "adult") %>%
      # check the age at the vital measurement
      mutate(date_age_check = sql(calc_days_between_dates("birth_date", "measure_date"))) %>%
      mutate(age_filter = case_when(days_old_at_index <= 7670 & days_old_at_index >= 6940 ~ 6940,
                                    TRUE ~ child_age_cutoff)) %>%
      # make sure age_filter is an integer
      mutate(age_filter = as.integer(age_filter)) %>% 
      # filter out any measurement (either ht or wt) that was taken below the pediatric age cutoff of 7305 days or 20 years old
      filter(date_age_check >= child_age_cutoff) %>%
      compute_new(indexes = list("patid"))
    
    # Infant: Pick the most recent BMI per patient (closest to index_date) ----
    
    
    if (!infant_vitals_less_than_2 %>% count() %>% pull(n) == 0) {
      echo_text("Step 4: Getting the most recent BMI closest to index for infants")
      # get closest height measurement for infants
      closest_ht_infant_less_than_2 <-infant_vitals_less_than_2 %>%
        mutate(ht = as.numeric(ht)) %>% 
        # exclude any missing or ht < 0
        filter(!is.na(ht) & ht > 0) %>%
        # find the number of days between measurement and index date
        mutate(days_diff_ht = sql(calc_days_between_dates("index_date", "measure_date"))) %>%
        # make sure days_diff_ht is an integer
        mutate(days_diff_ht = as.integer(days_diff_ht)) %>% 
        # limit so that measurements are in the time period of interest
        filter(days_diff_ht <= 0 & days_diff_ht >= infant_ht_days) %>%
        # group by patid and find measurement closest to zero then ungroup
        group_by(patid) %>%
        slice_max(order_by = days_diff_ht,
                  n = 1,
                  with_ties = FALSE) %>%  # smallest negative = closest to 0
        ungroup() %>%
        compute_new(indexes = list("patid"))
      
      
      # get closest weight measurement for infants
      closest_wt_infant_less_than_2 <-infant_vitals_less_than_2 %>%
        mutate(wt = as.numeric(wt)) %>% 
        # exclude any missing or wt <= 0
        filter(!is.na(wt) & wt > 0) %>%
        # find the number of days between measurement and index date
        mutate(days_diff_wt = sql(calc_days_between_dates("index_date", "measure_date"))) %>%
        # limit so that measurements are in the time period of interest then ungroup
        filter(days_diff_wt <= 0 & days_diff_wt >= infant_wt_days) %>%
        # group by patid and find measurement closest to zero
        group_by(patid) %>%
        slice_max(order_by = days_diff_wt,
                  n = 1,
                  with_ties = FALSE) %>%  # smallest negative = closest to 0
        ungroup() %>%
        compute_new(indexes = list("patid"))
      
      
      # join height and weight into a single row for each patient
      closest_bmi_infant_less_than_2  <- closest_ht_infant_less_than_2 %>%
        select(patid, index_date, ht, days_diff_ht, measure_date_ht = measure_date, sex, days_old_at_index, bmi_age) %>%
        full_join(
          closest_wt_infant_less_than_2 %>%
            select(patid, index_date, wt, days_diff_wt, measure_date_wt = measure_date, sex, days_old_at_index, bmi_age),
          by = c('patid', 'index_date', 'sex', 'days_old_at_index', 'bmi_age')
        )  %>%
        
        # filter out patients that are missing ht or wt because we can only compute BMI with both
        filter(!is.na(ht) & !is.na(wt)) %>%
        
        # only include male or female sex
        filter(sex == "M" | sex == "F") %>%
        
        # recode so that it works with the cdcanthro function
        mutate(sex = case_when(sex == "M" ~ 1L, sex == "F" ~ 2L, TRUE ~ NA_integer_)) %>%
        
        # convert days old to months old so that it works with the cdcanthro function
        # mutate(months_old_at_index = (days_old_at_index / 30.4375)) %>%
        
        # calculate bmi using formula for inches and pounds (default PCORnet height and weight units)
        mutate(bmi_comp = as.numeric((wt / (ht^2)) * 703)) %>%
        
        # convert height to cm and weight to kilograms so that it works within cdcanthro function
        mutate(ht_cm = as.numeric(ht*2.54), wt_kg = as.numeric(wt/2.205)) %>%
        compute_new(indexes = list("patid"))
      
      
      
      # Infant: Performing final BMI calculation and assigning categories -------
      
      if (!closest_bmi_infant_less_than_2 %>% count() %>% pull(n) == 0) {
        echo_text("Step 5: Performing final BMI calculation and assigning categories for infants")
        
        # Get final BMI for infants
        bmi_tbl_infant_less_than_2 <-
          closest_bmi_infant_less_than_2 %>%
          
          # collect in order to do the computations
          collect_new() %>%
   
          # has to be saved as a dataframe to work within cdcanthro function
          as.data.table()
        
        if (!bmi_tbl_infant_less_than_2 %>% count() %>% pull(n) == 0) {
        echo_text("...running whoanthro")
        whoanthro_rslt <-
          whoanthro(
            bmi_tbl_infant_less_than_2,
            agedays = days_old_at_index,
            wt = wt_kg,
            lenhei = ht_cm,
            headc = NA,
            bmi = bmi_comp
          ) %>%
          as_tibble()
        
        if (!whoanthro_rslt %>% count() %>% pull(n) == 0) {
          echo_text("...whoanthro complete")
          echo_text("...categorize BMI for infants")
          bmi_cat_rslt$bmi_cat_infant_less_than_2  <-
            whoanthro_rslt %>%
            mutate(bmip = as.numeric(round(pnorm(bmiz), 4) * 100), 
                   bmiz = as.numeric(bmiz)) %>%
            mutate(
              bmi_category = case_when(
                #     # anything that is NA will be labelled as missing
                is.na(bmip) ~ "Infant <2 yrs: Missing",
                bmiz > 5 | bmiz <(-5) ~ "Infant <2 yrs: Biologically implausible",
                bmip >= 0 & bmip <2.3 ~ "Infant <2 yrs: Underweight",
                bmip >= 2.3 & bmip <97.7 ~ "Infant <2 yrs: Normal weight",
                bmip >= 97.7 & bmip <99 ~ "Infant <2 yrs: Overweight",
                bmip >= 99 & bmip <= 100 ~ "Infant <2 yrs: Severely Obese",
                #     # everything else labelled as missing
                TRUE ~ "Infant <2 yrs: Missing"
              )
            ) %>%
            distinct(patid, index_date, measure_date_ht, measure_date_wt, ht, wt, bmi_comp, bmi_category, bmi_age) %>%
            copy_to_new(name = "infant_bmi", df = ., indexes = list("patid"))
        }else{
          echo_text("...cdcanthro returned blank table")
        }
      }else{
        echo_text("...no BMI measurements")
      }
      } else{
        echo_text("Step 5: Skipping step since no valid ht and wt records for infants found")
      }
      
    } else{
      echo_text("Step 4: Skipping step since no infants in cohort")
      echo_text("Step 5: Skipping step since no infants in cohort")
    }
    # Pediatric: Pick the most recent BMI per patient (closest to index_date) ----
    
    
    if (!child_vitals_2_to_19 %>% count() %>% pull(n) == 0) {
      echo_text("Step 6: Getting the most recent BMI closest to index for pediatrics")
      
      # get closest height measurement for pediatrics
      closest_ht_child_2_to_19 <-child_vitals_2_to_19 %>%
        mutate(ht = as.numeric(ht)) %>%
        # exclude any missing or ht < 0
        filter(!is.na(ht) & ht > 0) %>%
        # find the number of days between measurement and index date
        mutate(days_diff_ht = sql(calc_days_between_dates("index_date", "measure_date"))) %>%
        mutate(days_diff_ht = as.integer(days_diff_ht)) %>% 
        # limit so that measurements are in the time period of interest
        filter(days_diff_ht <= 0 & days_diff_ht >= child_ht_days) %>%
        # group by patid and find measurement closest to zero then ungroup
        group_by(patid) %>%
        slice_max(order_by = days_diff_ht,
                  n = 1,
                  with_ties = FALSE) %>%  # smallest negative = closest to 0
        ungroup() %>%
        compute_new(indexes = list("patid"))
      
      
      # get closest weight measurement for pediatrics
      closest_wt_child_2_to_19 <-child_vitals_2_to_19 %>%
        mutate(wt = as.numeric(wt)) %>% 
        # exclude any missing or wt <= 0
        filter(!is.na(wt) & wt > 0) %>%
        # find the number of days between measurement and index date
        mutate(days_diff_wt = sql(calc_days_between_dates("index_date", "measure_date"))) %>%
        mutate(days_diff_wt = as.integer(days_diff_wt)) %>% 
        # limit so that measurements are in the time period of interest then ungroup
        filter(days_diff_wt <= 0 & days_diff_wt >= child_wt_days) %>%
        # group by patid and find measurement closest to zero
        group_by(patid) %>%
        slice_max(order_by = days_diff_wt,
                  n = 1,
                  with_ties = FALSE) %>%  # smallest negative = closest to 0
        ungroup() %>%
        compute_new(indexes = list("patid"))
      
      
      # join height and weight into a single row for each patient
      closest_bmi_child_2_to_19 <- closest_ht_child_2_to_19 %>%
        select(patid, index_date, ht, days_diff_ht, measure_date_ht = measure_date, sex, days_old_at_index, bmi_age) %>%
        full_join(
          closest_wt_child_2_to_19 %>%
            select(patid, index_date, wt, days_diff_wt, measure_date_wt = measure_date, sex, days_old_at_index, bmi_age),
          by = c('patid', 'index_date', 'sex', 'days_old_at_index', 'bmi_age')
        )  %>%
        # filter out patients that are missing ht or wt because we can only compute BMI with both
        filter(!is.na(ht) & !is.na(wt)) %>%
        
        # only include male or female sex
        filter(sex == "M" | sex == "F") %>%
        
        # recode so that it works with the cdcanthro function
        mutate(sex = case_when(sex == "M" ~ 1L, sex == "F" ~ 2L, TRUE ~ NA_integer_)) %>%
        
        # convert days old to months old so that it works with the cdcanthro function
        mutate(months_old_at_index = as.numeric(days_old_at_index / 30.4375)) %>%
        
        # calculate bmi using formula for inches and pounds (default PCORnet height and weight units)
        mutate(bmi_comp = as.numeric((wt / (ht^2)) * 703)) %>%
        
        # convert height to cm and weight to kilograms so that it works within cdcanthro function
        mutate(ht_cm = as.numeric(ht*2.54), wt_kg = as.numeric(wt/2.205)) %>%
        
        # the program will also filter out anyone who does not fall within this range
        filter(months_old_at_index >= 24L,
               months_old_at_index < 240L) %>%
        compute_new(indexes = list("patid"))
      
      
      
      # Pediatric: Performing final BMI calculation and assigning categories -------
      
      
      if (!closest_bmi_child_2_to_19 %>% count() %>% pull(n) == 0) {
        echo_text("Step 7: Performing final BMI calculation and assigning categories for pediatrics")
        
        
        # Get final BMI for pediatrics
        bmi_tbl_child_2_19 <-
          closest_bmi_child_2_to_19 %>%
          
          # collect in order to do the computations
          collect_new() %>%
          
          # has to be saved as a dataframe to work within cdcanthro function
          as.data.table()
        
        if (!bmi_tbl_child_2_19 %>% count() %>% pull(n) == 0) {
        echo_text("...running cdcanthro")
        cdcanthro_rslt <-
          cdcanthro(
            bmi_tbl_child_2_19,
            age = months_old_at_index,
            wt = wt_kg,
            ht = ht_cm,
            bmi = bmi_comp,
            all = FALSE
          ) %>%
          as_tibble()
        
        if (!cdcanthro_rslt %>% count() %>% pull(n) == 0) {
          echo_text("...cdcanthro computation complete")
          echo_text("...categorize BMI for pediatrics")
          bmi_cat_rslt$bmi_cat_child_2_19 <-
            cdcanthro_rslt %>%
            mutate(
              mod_bmiz = as.numeric(mod_bmiz),
              bmip = as.numeric(bmip),
              bmip95 = as.numeric(bmip95)
            ) %>% 
            mutate(
              bmi_category = case_when(
                # anything that is NA will be labelled as missing
                is.na(bmip) ~ "Pediatric 2-19 yrs: Missing",
                # biologically implausible z-scores https://www.cdc.gov/growth-chart-training/hcp/computer-programs/sas-who.html
                mod_bmiz< (-4) | mod_bmiz >8 ~ "Pediatric 2-19 yrs: Biologically implausible",
                # child & teen BMI categories https://www.cdc.gov/bmi/child-teen-calculator/bmi-categories.html
                bmip >= 0 & bmip < 5  ~ "Pediatric 2-19 yrs: Underweight (<5th percentile)",
                bmip >= 5 & bmip < 85 ~ "Pediatric 2-19 yrs: Normal weight (>=5th - <85th percentile)",
                bmip >= 85 & bmip < 95 ~ "Pediatric 2-19 yrs: Overweight (>=85th - <95th percentile)",
                bmip >= 95 & bmip95 < 120 ~ "Pediatric 2-19 yrs: Obesity (>=95th percentile & <120% of 95th percentile)",
                bmip >= 95 & bmip95 >= 120 & bmip95 < 140   ~ "Pediatric 2-19 yrs: Obese class 2 (>=120% & < 140% of the 95th percentile or greater)",
                bmip >= 95 & bmip95 >= 140  ~ "Pediatric 2-19 yrs: Obese class 3 (>=140% of the 95th percentile or greater)",
                
                # everything else labelled as missing
                TRUE ~ "Pediatric 2-19 yrs: Missing"
              )
            ) %>%
            distinct(patid, index_date, measure_date_ht, measure_date_wt, ht, wt, bmi_comp, bmi_category, bmi_age) %>%
            copy_to_new(name = "ped_bmi", df = ., indexes = list("patid"))
        }else{
          echo_text("...cdcanthro returned blank table")
        }
        }else{
          echo_text("...no BMI measurements")
        }
      } else{
        echo_text("Step 7: Skipping step since no valid ht and wt records for pediatrics found")
      }
      
    } else{
      echo_text("Step 6: Skipping step since no pediatrics in cohort")
      echo_text("Step 7: Skipping step since no pediatrics in cohort")
    }
    # Adult: Pick the most recent BMI per patient (closest to index_date) --------
    
    
    if (!adult_vitals %>% count() %>% pull(n) == 0) {
      echo_text("Step 8: Getting the most recent BMI closest to index for adults")
      # get closest height measurement for adults
      closest_ht_adult <- adult_vitals %>%
        mutate(ht = as.numeric(ht)) %>% 
        # exclude any missing or ht < 0
        filter(!is.na(ht) & ht > 0) %>%
        # find the number of days between measurement and index date
        mutate(days_diff_ht = sql(calc_days_between_dates("index_date", "measure_date"))) %>%
        mutate(days_diff_ht = as.integer(days_diff_ht)) %>% 
        # limit so that measurements are in the time period of interest
        filter(days_diff_ht <= 0 & days_diff_ht >= adult_ht_days) %>%
        # group by patid and find measurement closest to zero then ungroup
        group_by(patid) %>%
        slice_max(order_by = days_diff_ht,
                  n = 1,
                  with_ties = FALSE) %>%  # smallest negative = closest to 0
        ungroup() %>%
        compute_new(indexes = list("patid"))
      
      
      # get closest weight measurement for adults
      closest_wt_adult <-	adult_vitals %>%
        mutate(wt = as.numeric(wt)) %>% 
        # exclude any missing or wt < 0
        filter(!is.na(wt) & wt > 0) %>%
        # find the number of days between measurement and index date
        mutate(days_diff_wt = sql(calc_days_between_dates("index_date", "measure_date"))) %>%
        mutate(days_diff_wt = as.integer(days_diff_wt)) %>% 
        # limit so that measurements are in the time period of interest then ungroup
        filter(days_diff_wt <= 0 & days_diff_wt >= adult_wt_days) %>%
        # group by patid and find measurement closest to zero
        group_by(patid) %>%
        slice_max(order_by = days_diff_wt,
                  n = 1,
                  with_ties = FALSE) %>%  # smallest negative = closest to 0
        ungroup() %>%
        compute_new(indexes = list("patid"))
      
      
      # join height and weight into a single row for each patient
      closest_bmi_adult <-
        closest_ht_adult %>%
        select(patid, index_date, ht, days_diff_ht, measure_date_ht = measure_date, bmi_age) %>%
        full_join(
          closest_wt_adult %>%
            select(patid, index_date, wt, days_diff_wt, measure_date_wt = measure_date, bmi_age),
          by = c('patid', 'index_date', 'bmi_age')
        )  %>%
        
        # filter out patients that are missing ht or wt because we can only compute BMI with both
        filter(!is.na(ht) & !is.na(wt)) %>%
        
        compute_new(indexes = list("patid"))
      
      
      # Adult: Performing final BMI calculation and assigning categories -----------
      
      if (!closest_bmi_adult %>% count() %>% pull(n) == 0) {
        echo_text("Step 9: Performing final BMI calculation and assigning categories for adults")
        
        # Get final BMI for adults
        bmi_cat_rslt$bmi_tbl_adult <-closest_bmi_adult %>%
          
          # calculate BMI and then assign categories
          mutate(
            bmi_comp = as.numeric((wt / (ht^2)) * 703),
            bmi_category = case_when(
              is.na(bmi_comp)                  ~ "Adult: Missing",
              # flag biologically implausible https://www.cdc.gov/dnpao-data-trends-maps/database/definitions.html#:~:text=Two%20indicators%20for%20adults%2018,Pregnant%20respondents.
              bmi_comp < 12 | bmi_comp >= 100  ~ "Adult: Biologically implausible",
              bmi_comp >= 12 & bmi_comp < 18.5 ~ "Adult: Underweight (<18.5 kg/m2)",
              bmi_comp >= 18.5 & bmi_comp < 25 ~ "Adult: Normal weight (18.5 - <25 kg/m2)",
              bmi_comp >= 25 & bmi_comp < 30   ~ "Adult: Overweight (25 - <30 kg/m2)",
              bmi_comp >= 30 & bmi_comp < 35   ~ "Adult: Obese class 1 (30 - <35 kg/m2)",
              bmi_comp >= 35 & bmi_comp < 40   ~ "Adult: Obese class 2 (35 - <40 kg/m2)",
              bmi_comp >= 40 & bmi_comp < 100 ~ "Adult: Obese class 3 (>=40 kg/m2)",
              TRUE                              ~ "Adult: Missing")
          ) %>%
          distinct(patid, index_date, measure_date_ht, measure_date_wt, ht, wt, bmi_comp, bmi_category, bmi_age) %>%
          compute_new(indexes = list("patid"))
      } else{
        echo_text("Step 9: Skipping step since no valid ht and wt records for adults found")
      }
    } else{
      echo_text("Step 8: Skipping step since no adults in cohort")
      echo_text("Step 9: Skipping step since no adults in cohort")
    } 	
  } else {
    echo_text("Step 3 to 9: Skipping steps since vital table contains no records")
  }
  # Join pediatric and adult BMI tables together -------------------------------
  
  if (length(bmi_cat_rslt) > 0) {
    echo_text("Step 10: Join infant, pediatric, and adult BMI tables together")
    # full join if list isn't empty
    bmi_tbl_final <- reduce(bmi_cat_rslt, union_all) %>%
      select(patid, bmi_category, bmi_age) %>%
      compute_new(indexes = list("patid"))
  } else {
    echo_text("Step 10: No records were found")
    # create dummy table in case an empty cohort was fed into function
    bmi_tbl_final <- 
      tibble(patid = character(), bmi_category = character(), bmi_age = character()) %>% 
      copy_to_new(name = "bmi_final_blank", df = ., indexes = list("patid"))
  } 
  # Return result --------------------------------------------------------------
  
  return(bmi_tbl_final)
  
  
}