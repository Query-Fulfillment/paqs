# Original define_criteria function and mapping functions

#' Define a clinical criteria
#'
#' This function dispatches to the appropriate method based on the codetype in the codeset.
#'
#' @param codeset A tibble containing at least a 'codetype' character column.
#' @param start_date The starting date for the analysis.
#' @param end_date The ending date for the analysis.
#' @param n_code The number of codes to use.
#' @param n_days The number of days separation to consider.
#'
#' @return A result specific to the chosen method.
#' @export
define_criteria <- function(cohort = NULL, codeset, start_date, end_date, n_code_req = 1, days_sep = 0, qual_event = 'first', criterion_suffix) {
	# Validate inputs
	if (!is.null(codeset)) {
		if (!is.tbl(codeset)) {
			stop("codeset must be a data frame or tibble.")
		}
	}

	if (!"codetype" %in% colnames(codeset)) {
		stop("codeset must contain a column named 'codetype'.")
	}

	# Get the codetype to determine which method to use
	if (!is.null(codeset)) {
		codetype <- match_codetype_to_table() %>%
			inner_join(codeset) %>%
			distinct(table) %>%
			pull()
	}

	# Check if there are multiple codetypes
	if (length(codetype) > 1) {
		warning("Multiple codetypes found in codeset. Will use the first one for method dispatch.")
		codetype <- codetype[1]
	}

	# Create a new class for dispatching
	class(codeset) <- c(codetype, class(codeset))

	# Use S3 method dispatch
	UseMethod("define_criteria", codeset)
}

match_codetype_to_table <- function(codeset) {
	# Valid values correspond to type values allowed in each of the following PCORnet CDM tables:
	tibble::tribble(~codetype,~table,
									# diagnosis:
									'DX09', 'diagnosis',
									'DX10', 'diagnosis',
									'DX11', 'diagnosis',
									'DXSM', 'diagnosis',
									# dispensing:
									'RX01','dispensing',
									'RX11','dispensing',
									'RX09','dispensing',
									# procedure:
									'PX09', 'procedure',
									'PX10', 'procedure',
									'PX11', 'procedure',
									'PXCH', 'procedure',
									'PXLC', 'procedure',
									'PXND', 'procedure',
									'PXRE', 'procedure',
									# prescribing:
									'PR00', 'prescribing',
									# Lab_result_cm:
									'LBLC', 'lab_result_cm',
									'LBCH', 'lab_result_cm',
									'LB09', 'lab_result_cm',
									'LB10', 'lab_result_cm',
									'LB11', 'lab_result_cm',
									# med_admin:
									'MA09', 'med_admin',
									'MA11', 'med_admin',
									'MA00', 'med_admin',
									# obs_clin:
									'OCSM', 'obs_clin',
									'OCLC', 'obs_clin',
									# immunization:
									'VXCX', 'immunization',
									'VXND', 'immunization',
									'VXCH', 'immunization',
									'VXRX', 'immunization',
									# death :
									'DTH', 'death') %>%
		copy_to_new(df = .,name = "crosswalk",overwrite = TRUE,temporary = TRUE)
}

# Helper functions for S3 methods

#' Apply date filters
#'
#' @param cohort_data The cohort data with event codes
#' @param col1 The date column to be filtered on
#' @param col2 The date column to be coalaced before applying filter logic
#' @param event_date_col The name of the event date column (e.g., "dx_date", "px_date")
#' @param start_date The starting date or a variable name holding date for filtering
#' @param end_date The ending date for variable name holding date filtering
#'
#' @return A tibble with coalesced dates and filtered records
apply_date_filters <- function(cohort_data, col1, col2 = NULL, event_code_col,
																 start_date, end_date, criterion_suffix, ...) {

	coalesced_date_col_name <- paste0('criterion_',criterion_suffix, "_date")
	criterion_encounterid <- paste0('encounterid_',criterion_suffix)

	  start_val <- resolve_date_input(start_date)
    end_val   <- resolve_date_input(end_date)
	
	# Trying coalescing
	try_coalescing <- try(
		{
			cohort_in_query_period <- cohort_data %>%
      	mutate(!!sym(coalesced_date_col_name) := if(!is.null(col2)) { coalesce(!!sym(col2), !!sym(col1)) } else {!!sym(col1)}) %>%
      		filter(.,(!!sym(coalesced_date_col_name)) >= !!start_val & (!!sym(coalesced_date_col_name)) <= !!end_val) %>%
      			distinct(patid, encounterid, !!sym(event_code_col), !!sym(coalesced_date_col_name)) %>%
				     rename(!!sym(criterion_encounterid)  := encounterid) %>%
      				compute_new(.,indexes = list('patid'))
		},
		silent = TRUE
	)

	# Checking if coalescing try step returned an error
	if (all(class(try_coalescing) != "try-error")) {
		echo_text(paste("Coalesced", col1, "and ", col2))

		# Defensive coding to ensure that if coalescing step failed silently and produced no patients. 
		# Could also be that no patients qualified but, TRUE logic on this line will run an additional step to ensure that if if col1 gets a return
		if (distinct_ct(cohort_in_query_period, id_col = "patid") == 0) {
			echo_text(paste(
				"Coalescing", col1, "and ", col2,
				"yielded no patients defaulting to just admit_dates"
			))
			cohort_in_query_period <- cohort_data %>%
				mutate(!!sym(coalesced_date_col_name) := !!sym(col1)) %>%
				 filter(!!sym(coalesced_date_col_name) >= !!start_val & !!sym(coalesced_date_col_name) <= !!end_val) %>%
				  distinct(patid, encounterid, !!sym(event_code_col), !!sym(col1), !!sym(coalesced_date_col_name)) %>% 
				   rename(!!sym(criterion_encounterid)  := encounterid) 
		}
		else {
			return(cohort_in_query_period)
		}
	} else { # Fail-safe to just work 
		echo_text(paste(
			"Coalescing", col1, "and ", col2, "failed - defaulting to just admit_dates"
		))
		cohort_in_query_period <- cohort_data %>%
			mutate(!!sym(coalesced_date_col_name) := !!sym(col1)) %>%
			 filter(!!sym(coalesced_date_col_name) >= !!start_val & !!sym(coalesced_date_col_name) <= !!end_val) %>%
			  distinct(patid, encounterid, !!sym(event_code_col), !!sym(col1), !!sym(coalesced_date_col_name)) %>% 
					rename(!!sym(criterion_encounterid)  := encounterid) 
	}

	return(cohort_in_query_period)
}

#' Create summary of distinct coalesced dates by patient
#'
#' @param cohort_data The cohort data with coalesced dates
#' @param coalesced_col The name of the coalesced date column
#' @param n_code_req The minimum number of distinct dates required
#'
#' @return A tibble with patient summaries meeting the code requirement
obtain_first_last_events <- function(cohort_data, date_col, n_code_req) {

	first_col_name <- paste0("first_", date_col)
	last_col_name <- paste0("last_", date_col)

	cohort_data %>%
		group_by(patid) %>%
		summarize(
			distinct_dates = n_distinct(!!sym(date_col)),
			!!sym(first_col_name) := min(!!sym(date_col), na.rm = TRUE),
			!!sym(last_col_name) := max(!!sym(date_col), na.rm = TRUE)
		) %>%
		ungroup() %>%
		filter(distinct_dates >= n_code_req)
}

#' Apply days separation logic
#'
#' @param summary_data The summary data from obtain_first_last_events
#' @param cohort_data The original cohort data
#' @param coalesced_col The name of the coalesced date column
#' @param start_date The starting date for filtering
#' @param end_date The ending date for filtering
#' @param days_sep The minimum days separation required
#' @param qual_event First, Last or Random between start_date and end_date
#'
#' @return A tibble with patients meeting the days separation requirement
apply_days_separation <- function(summary_data, cohort_data, date_col,
																	start_date, end_date, days_sep, qual_event, 
																	encounterid_criterion,...) {

	first_col_name <- paste0("first_", date_col)
	last_col_name <- paste0("last_", date_col)

	start_val <- resolve_date_input(start_date)
  end_val   <- resolve_date_input(end_date)
	
	summarized <- summary_data %>%
		inner_join(cohort_data) %>%
		  mutate(
			  days_sep_from_first = sql(calc_days_between_dates(first_col_name, date_col)),
			  days_sep_from_last = sql(calc_days_between_dates(date_col, last_col_name))) %>%
		# Correcting days_sep to include the date of the event for case when only two visits
		  mutate(
			  days_sep_from_first = ifelse(days_sep_from_first == 0 & days_sep_from_last != 1, 1, days_sep_from_first),
			  days_sep_from_last = ifelse(days_sep_from_last == 0 & days_sep_from_first != 1, 1, days_sep_from_last)) %>%
		     filter(days_sep_from_first >= days_sep & days_sep_from_last >= days_sep)
	
slice_func <- switch(
  qual_event,
  first = dplyr::slice_min,
  last = dplyr::slice_max
)

rslt <- summarized %>% 
	group_by(patid) %>% 
	slice_func(!!sym(date_col),with_ties = FALSE) %>%
	ungroup() %>% 
	select(patid, !!sym(encounterid_criterion) , all_of(date_col))


	rslt
}

#' Validate final cohort and provide messaging
#'
#' @param final_cohort The final cohort data
#'
#' @return The validated cohort (unchanged)
validate_final_cohort <- function(final_cohort) {
	patient_count <- distinct_ct(final_cohort, id_col = "patid")

	if (patient_count == 0) {
		echo_text(
			"Warning: No Patient Qualified the cohort definition. Query Table 1 may return empty or zero counts. Based on the cohort definition if you expect to see patients at your datamart, please report back to qf@pcornet.org with this finding"
		)
	} else if (patient_count >= 1) {
		echo_text(
			"Cohort development returned with non-zero patient/s....further computations can be continued"
		)
	}

	return(final_cohort)
}

# S3 methods using the helper functions

#' Define Criteria for Diagnosis Event
#'
#' This function creates a criteria based on diagnosis codes.
#'
#' @param codeset A tibble containing diagnosis codes.
#' @param start_date The starting date for the analysis.
#' @param end_date The ending date for the analysis.
#' @param n_code_req The number of codes to use.
#' @param days_sep The number of days to consider.
#'
#' @return A tibble or Remote table with patid | encounterid | dx | criteria_{tag}_date
#' @export
define_criteria.diagnosis <- function(cohort = NULL, codeset, start_date, end_date, n_code_req = 1, days_sep = 0, qual_event = 'first', criterion_suffix ) {
	# Implementation for diagnosis codes
	message("Processing diagnosis table...")

	if(n_code_req == 1) {
		message('n_code_req is set to 1, setting days_sep to 0 by default')
		days_sep <- 0
	}

	if (!is.null(cohort)) {
    input_tbl <- cdm_tbl('diagnosis') %>% inner_join(cohort, by = c('patid'))
	} else {
	  input_tbl <- cdm_tbl('diagnosis')
	}

	# Step 1 : getting atleast one occurrence of code of interest
	cohort_with_dx <- input_tbl %>%
		inner_join(codeset, by = c("dx" = "code"))

	echo_text("Filtered with atlest one dx code of interest")

	# Coalesce event dates
	cohort_with_dx_in_query_period <- apply_date_filters(
		cohort_data = cohort_with_dx, 
		col1 = "dx_date",
		col2 =  "admit_date", 
		event_code_col = "dx",
		start_date = start_date, 
		end_date =  end_date,
		criterion_suffix = criterion_suffix
	)

	echo_text("Filter patients with atlest one dx code of interest in the query period")

	# Create date summary
	distinct_coalesced_dx_admits_summary <- obtain_first_last_events(
		cohort_data = cohort_with_dx_in_query_period, 
	  date_col = paste0('criterion_',criterion_suffix, "_date"), 
	  n_code_req = n_code_req
	)

	echo_text("Filter patients with qualifying n_code_req criteria")

	# Apply days separation
	distinct_visit_requirement_pats <- apply_days_separation(
		summary_data = distinct_coalesced_dx_admits_summary, 
		cohort_data = cohort_with_dx_in_query_period,
		date_col = paste0('criterion_',criterion_suffix, "_date"), 
		start_date = start_date, 
		end_date = end_date, 
		days_sep = days_sep,
		qual_event = qual_event,
		encounterid_criterion = paste0('encounterid_',criterion_suffix)
	)

	# Validate and return
	return(validate_final_cohort(distinct_visit_requirement_pats))
}

#' Define Healthcare Exposure Index for Procedure Codes
#'
#' This function creates a healthcare exposure index based on procedure codes.
#'
#' @param codeset A tibble containing procedure codes.
#' @param start_date The starting date for the analysis.
#' @param end_date The ending date for the analysis.
#' @param n_code The number of codes to use.
#' @param n_days The number of days to consider.
#'
#' @return A data structure representing the healthcare exposure index for procedures.
#' @export
define_criteria.procedure <- function(cohort = NULL, codeset, start_date, end_date, n_code_req = 1, days_sep = 0, qual_event = 'first', criterion_suffix ) {
# Implementation for diagnosis codes
	message("Processing procedures table...")

	if(n_code_req == 1) {
		message('n_code_req is set to 1, setting days_sep to 0 by default')
		days_sep <- 0
	}

	if (!is.null(cohort)) {
    input_tbl <- cdm_tbl('procedures') %>% inner_join(cohort, by = c('patid'))
	} else {
	  input_tbl <- cdm_tbl('procedures')
	}

	# Step 1 : getting atleast one occurrence of code of interest
	cohort_with_px <- input_tbl %>%
		inner_join(codeset, by = c("px" = "code"))

	echo_text("Filtered with atlest one px code of interest")

	# Coalesce event dates
	cohort_with_px_in_query_period <- apply_date_filters(
		cohort_data = cohort_with_px, 
		col1 = "px_date",
		col2 =  "admit_date", 
		event_code_col = "px",
		start_date = start_date, 
		end_date =  end_date,
		criterion_suffix = criterion_suffix
	)

	echo_text("Filter patients with atlest one px code of interest in the query period")

	# Create date summary
	distinct_coalesced_px_admits_summary <- obtain_first_last_events(
		cohort_data = cohort_with_px_in_query_period, 
	  date_col = paste0('criterion_',criterion_suffix, "_date"), 
	  n_code_req = n_code_req
	)

	echo_text("Filter patients with qualifying n_code_req criteria")

	# Apply days separation
	distinct_visit_requirement_pats <- apply_days_separation(
		summary_data = distinct_coalesced_px_admits_summary, 
		cohort_data = cohort_with_px_in_query_period,
		date_col = paste0('criterion_',criterion_suffix, "_date"), 
		start_date = start_date, 
		end_date = end_date, 
		days_sep = days_sep,
		qual_event = qual_event,
		encounterid_criterion = paste0('encounterid_',criterion_suffix)
	)

	# Validate and return
	return(validate_final_cohort(distinct_visit_requirement_pats))
}

#' Default method for define_criteria
#'
#' @param codeset A tibble containing codes.
#' @param start_date The starting date for the analysis.
#' @param end_date The ending date for the analysis.
#' @param n_code The number of codes to use.
#' @param n_days The number of days to consider.
#'
#' @return An error message.
#' @export
define_criteria.default <- function(codeset, start_date, end_date, n_code, n_days, qual_event) {
	cli_abort(
		c("x" = "Error: Unknown codetype value",
			"i" = "PAQS permitted values :",
			"i" =	"diagnosis:",
			'- DX09',
			'- DX10',
			'- DX11',
			'- DXSM',
			"i" ="dispensing:",
			'- RX01',
			'- RX11',
			'- RX09',
			"i" =	"procedure:",
			'- PX09',
			'- PX10',
			'- PX11',
			'- PXCH',
			'- PXLC',
			'- PXND',
			'- PXRE',
			"i" =	'prescribing:',
			'- PR00',
			"i" =	'- lab_result_cm:',
			'- LBLC',
			'- LBCH',
			'- LB09',
			'- LB10',
			'- LB11',
			"i" ='med_admin:',
			'- MA09',
			'- MA11',
			'- MA00',
			"i" ='obs_clin:',
			'- OCSM',
			'- OCLC',
			"i" ="immunization:",
			'- VXCX',
			'- VXND',
			'- VXCH',
			'- VXRX',
			'death:',
			'- DTH')
	)
}


resolve_date_input <- function(x) {
  if (inherits(x, "Date")) {
    x
  } else if (is.character(x)) {
    # Try parsing using multiple common formats
    parsed <- suppressWarnings(parse_date_time(x, orders = c(
      "Ymd", "mdY", "dmY", "Y-m-d", "m/d/Y", "d/m/Y",
      "Y/m/d", "Y.m.d", "B d, Y", "d B Y"
    )))
    if (!is.na(parsed)) {
      as.Date(parsed)
    } else {
      sym(x)  # Must be a column name
    }
  } else if (is_symbol(x)) {
    x
  } else {
    abort("start_date / end_date must be either a date or a column name")
  }
}
