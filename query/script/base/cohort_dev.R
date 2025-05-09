#' Define Health Event of Interest (HEI)
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
define_hei <- function(codeset, start_date, end_date, n_code_req, days_sep) {
  # Validate inputs
  if (!is.tbl(codeset)) {
    stop("codeset must be a data frame or tibble.")
  }

  if (!"codetype" %in% colnames(codeset)) {
    stop("codeset must contain a column named 'codetype'.")
  }

  # Get the codetype to determine which method to use
  codetype <- match_codetype_to_table() %>%
  	inner_join(codeset) %>%
  	distinct(table) %>%
  	pull()

  # Check if there are multiple codetypes
  if (length(codetype) > 1) {
    warning("Multiple codetypes found in codeset. Will use the first one for method dispatch.")
    codetype <- codetype[1]
  }

  # Create a new class for dispatching
  class(codeset) <- c(codetype, class(codeset))

  # Use S3 method dispatch
  UseMethod("define_hei", codeset)
}

#' Define Healthcare Exposure Index for Diagnosis Codes
#'
#' This function creates a healthcare exposure index based on diagnosis codes.
#'
#' @param codeset A tibble containing diagnosis codes.
#' @param start_date The starting date for the analysis.
#' @param end_date The ending date for the analysis.
#' @param n_code The number of codes to use.
#' @param n_days The number of days to consider.
#'
#' @return A data structure representing the healthcare exposure index for diagnoses.
#' @export
define_hei.diagnosis <- function(codeset, start_date, end_date, n_code_req, days_sep) {
  # Implementation for diagnosis codes
  message("Processing diagnosis table...")

	if(n_code_req == 1) {
		message('n_code_req is set to 1, setting days_sep to 0 by default')
		days_sep <- 0
		}

	cohort_with_dx <- cdm_tbl('diagnosis') %>%
		inner_join(codeset, by = c("dx" = "code"))

	echo_text("Filter with atlest one dx code of interest")

	# Trying coalesing
	try_coalesing <- try(
		{
			cohort_with_dx_in_query_period <- cohort_with_dx %>%
				mutate(coalesced_dx_admit = coalesce(dx_date, admit_date)) %>%
				filter(coalesced_dx_admit >= start_date &
							 	coalesced_dx_admit <= end_date) %>%
				distinct(patid, encounterid, dx, admit_date, coalesced_dx_admit) %>%
				compute_new()
		},
		silent = T
	)

	if (all(class(try_coalesing) != "try-error")) {
		echo_text(
			"Coalesced dx_date and admit_date"
		)
		# Defensive coding if coalescing admit_date and dx_date runs but produces no patients
		if (distinct_ct(cohort_with_dx_in_query_period, id_col = "patid") == 0) {
			echo_text(
				"Coalescing admit_date and dx_date yeilded no patients defaulting to just admit_dates"
			)
			cohort_with_dx_in_query_period <- cohort_with_dx %>%
				mutate(coalesced_dx_admit = admit_date) %>%
				filter(coalesced_dx_admit >= start_date &
							 	coalesced_dx_admit <= end_date) %>%
				distinct(patid, encounterid, dx, admit_date, coalesced_dx_admit)
		}
	} else { # Fail-safe
		echo_text(
			"Coalescing dx_date and admit_date failed - defaulting to just admit_dates"
		)
		cohort_with_dx_in_query_period <- cohort_with_dx %>%
			mutate(coalesced_dx_admit = admit_date) %>%
			filter(coalesced_dx_admit >= start_date &
						 	coalesced_dx_admit <= end_date) %>%
			distinct(patid, encounterid, dx, admit_date, coalesced_dx_admit)
	}

	echo_text("Filter patients with atlest one dx code of interest in the query period")

	distinct_coalesced_dx_admits_summary <-
		cohort_with_dx_in_query_period %>%
		group_by(patid) %>%
		summarize(
			distinct_dates = n_distinct(coalesced_dx_admit),
			first_coalesced_dx_admit = min(coalesced_dx_admit, na.rm = TRUE),
			last_coalesced_dx_admit = max(coalesced_dx_admit, na.rm = TRUE)
		) %>%
		ungroup() %>%
		filter(distinct_dates >= n_code_req)

	echo_text("Filter patients with qualifying n_code_req criteria")

	distinct_visit_requirement_pats <-
		distinct_coalesced_dx_admits_summary %>%
		inner_join(cohort_with_dx_in_query_period) %>%
		filter(coalesced_dx_admit >= start_date &
					 	coalesced_dx_admit <= end_date) %>%
		mutate(
			days_sep_from_first = sql(
				calc_days_between_dates("first_coalesced_dx_admit", "coalesced_dx_admit")
			),
			days_sep_from_last = sql(
				calc_days_between_dates("coalesced_dx_admit", "last_coalesced_dx_admit")
			)
		) %>%
		# Correcting days_sep to include the date of the dx for case when only two visits which are seperated by atleast 1 day to allow enterance.
		mutate(
			days_sep_from_first = ifelse(
				days_sep_from_first == 0 &
					days_sep_from_last != 1,
				1,
				days_sep_from_first
			),
			days_sep_from_last = ifelse(
				days_sep_from_last == 0 &
					days_sep_from_first != 1,
				1,
				days_sep_from_last
			)
		) %>%
		filter(days_sep_from_first >= days_sep &
					 	days_sep_from_last >= days_sep) %>%
		distinct(patid, first_coalesced_dx_admit, last_coalesced_dx_admit)


	if (distinct_ct(distinct_visit_requirement_pats, id_col = "patid") == 0) {
		echo_text(
			"Warning:No Patient Qualified the cohort definition. Query Table 1 may return empty or zero counts. Based on the cohort definition if you expect to see patients at your datamart, please report back to qf@pcornet.org with this finding"
		)
	} else if (distinct_ct(distinct_visit_requirement_pats, id_col = "patid") >= 1) {
		echo_text(
			"Cohort development returned with non-zero patient/s....further computations can be continued"
		)
	}

	return(distinct_visit_requirement_pats)

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
define_hei.procedure <- function(codeset, start_date, end_date, n_code_req, days_sep) {
	# Implementation for diagnosis codes
	message("Processing diagnosis table...")

	if(n_code_req == 1) {
		message('n_code_req is set to 1, setting days_sep to 0 by default')
		days_sep <- 0
	}

	cohort_with_px <- cdm_tbl('procedures') %>%
		inner_join(codeset, by = c("px" = "code"))

	echo_text("Filter with atlest one px code of interest")

	# Trying coalesing
	try_coalesing <- try(
		{
			cohort_with_px_in_query_period <- cohort_with_px %>%
				mutate(coalesced_px_admit = coalesce(px_date, admit_date)) %>%
				filter(coalesced_px_admit >= start_date &
							 	coalesced_px_admit <= end_date) %>%
				distinct(patid, encounterid, px, admit_date, coalesced_px_admit) %>%
				compute_new()
		},
		silent = T
	)

	if (all(class(try_coalesing) != "try-error")) {
		echo_text(
			"Coalesced px_date and admit_date"
		)
		# Defensive coding if coalescing admit_date and px_date runs but produces no patients
		if (distinct_ct(cohort_with_px_in_query_period, id_col = "patid") == 0) {
			echo_text(
				"Coalescing admit_date and px_date yeilded no patients defaulting to just admit_dates"
			)
			cohort_with_px_in_query_period <- cohort_with_px %>%
				mutate(coalesced_px_admit = admit_date) %>%
				filter(coalesced_px_admit >= start_date &
							 	coalesced_px_admit <= end_date) %>%
				distinct(patid, encounterid, px, admit_date, coalesced_px_admit)
		}
	} else { # Fail-safe
		echo_text(
			"Coalescing px_date and admit_date failed - defaulting to just admit_dates"
		)
		cohort_with_px_in_query_period <- cohort_with_px %>%
			mutate(coalesced_px_admit = admit_date) %>%
			filter(coalesced_px_admit >= start_date &
						 	coalesced_px_admit <= end_date) %>%
			distinct(patid, encounterid, px, admit_date, coalesced_px_admit)
	}

	echo_text("Filter patients with atlest one px code of interest in the query period")

	distinct_coalesced_px_admits_summary <-
		cohort_with_px_in_query_period %>%
		group_by(patid) %>%
		summarize(
			distinct_dates = n_distinct(coalesced_px_admit),
			first_coalesced_px_admit = min(coalesced_px_admit, na.rm = TRUE),
			last_coalesced_px_admit = max(coalesced_px_admit, na.rm = TRUE)
		) %>%
		ungroup() %>%
		filter(distinct_dates >= n_code_req)

	echo_text("Filter patients with qualifying n_code_req criteria")

	distinct_visit_requirement_pats <-
		distinct_coalesced_px_admits_summary %>%
		inner_join(cohort_with_px_in_query_period) %>%
		filter(coalesced_px_admit >= start_date &
					 	coalesced_px_admit <= end_date) %>%
		mutate(
			days_sep_from_first = sql(
				calc_days_between_dates("first_coalesced_px_admit", "coalesced_px_admit")
			),
			days_sep_from_last = sql(
				calc_days_between_dates("coalesced_px_admit", "last_coalesced_px_admit")
			)
		) %>%
		# Correcting days_sep to include the date of the px for case when only two visits which are seperated by atleast 1 day to allow enterance.
		mutate(
			days_sep_from_first = ifelse(
				days_sep_from_first == 0 &
					days_sep_from_last != 1,
				1,
				days_sep_from_first
			),
			days_sep_from_last = ifelse(
				days_sep_from_last == 0 &
					days_sep_from_first != 1,
				1,
				days_sep_from_last
			)
		) %>%
		filter(days_sep_from_first >= days_sep &
					 	days_sep_from_last >= days_sep) %>%
		distinct(patid, first_coalesced_px_admit, last_coalesced_px_admit)


	if (distinct_ct(distinct_visit_requirement_pats, id_col = "patid") == 0) {
		echo_text(
			"Warning:No Patient Qualified the cohort definition. Query Table 1 may return empty or zero counts. Based on the cohort definition if you expect to see patients at your datamart, please report back to qf@pcornet.org with this finding"
		)
	} else if (distinct_ct(distinct_visit_requirement_pats, id_col = "patid") >= 1) {
		echo_text(
			"Cohort development returned with non-zero patient/s....further computations can be continued"
		)
	}

	return(distinct_visit_requirement_pats)
}

#' Default method for define_hei
#'
#' @param codeset A tibble containing codes.
#' @param start_date The starting date for the analysis.
#' @param end_date The ending date for the analysis.
#' @param n_code The number of codes to use.
#' @param n_days The number of days to consider.
#'
#' @return An error message.
#' @export
define_hei.default <- function(codeset, start_date, end_date, n_code, n_days) {
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

# You can add additional methods or helper functions as needed


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