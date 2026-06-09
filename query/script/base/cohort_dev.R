# Table configurations for different code types

#' Setting PCORnet or OMOP Table Configs
#'
#' @param cdm_type string value, `pcornet` | `omop`
#'
#' @returns
#'
#' @export
#' @examples
#' @title Set CDM Configuration
#' @description Set the global CDM type and table configurations for the session.
#' @param cdm_type Character string, either 'pcornet' or 'omop'.
#' @return No return value; populates `.GlobalEnv$cdm_type` and `.GlobalEnv$TABLE_CONFIGS`.
#' @export
#' @examples
#' set_cdm_config('pcornet')
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
				dispensing = list(
					table = "dispensing",
					code_column = "ndc",
					primary_date_column = "dispense_date",
					fallback_date_column = "dispense_date",
					permitted_codetype = c("RX01", "RX11", "RX09")
				),
				prescribing = list(
					table = "prescribing",
					code_column = "rxnorm_cui",
					primary_date_column = "rx_order_date",
					fallback_date_column = "rx_start_date",
					permitted_codetype = c("PR00")
				),
				med_admin = list(
					table = "med_admin",
					code_column = "medadmin_code",
					primary_date_column = "medadmin_start_date",
					fallback_date_column = "medadmin_start_date",
					permitted_codetype = c('MA09', 'MA11', 'MA00')
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
					primary_date_column = "obsclin_start_date",
					fallback_date_column = "obsclin_stop_date",
					permitted_codetype = c('OCSM', 'OCLC')
				),
				immunization = list(
					table = "immunization",
					code_column = "vx_code",
					primary_date_column = "vx_admin_date",
					fallback_date_column = "vx_record_date",
					permitted_codetype = c('VXCX', 'VXND', 'VXCH', 'VXRX')
				),
				death = list(
					table = "death",
					primary_date_column = "death_date",
					permitted_codetype = c('DTH')
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
# 		'dispensing',
# 		"",
# 		'RX11',
# 		'dispensing',
# 		"",
# 		'RX09',
# 		'dispensing',
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
# 		'prescribing',
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
# 		'med_admin',
# 		"",
# 		'MA11',
# 		'med_admin',
# 		"",
# 		'MA00',
# 		'med_admin',
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
		stop(
			paste0(
				"get_table_config() received a multi-table codeset: ",
				paste0(codetype_mapping, collapse = ", "),
				". This should be routed through define_criteria.multi()."
			)
		)
	}

	table_name <- codetype_mapping

	if (!table_name %in% names(TABLE_CONFIGS)) {
		stop(sprintf("No configuration found for table: %s", table_name))
	}

	return(TABLE_CONFIGS[[table_name]])
}

normalize_column_selection <- function(x) {
	if (is.null(x)) {
		return(character(0))
	}

	if (!is.character(x)) {
		cli_abort("Column selection arguments must be character vectors.")
	}

	unique(x)
}

validate_retained_columns <- function(
		codeset,
		cohort = NULL,
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
) {
	retain_codeset_cols <- normalize_column_selection(retain_codeset_cols)
	retain_cohort_cols <- normalize_column_selection(retain_cohort_cols)
	event_group_cols <- normalize_column_selection(event_group_cols)

	overlap_cols <- intersect(retain_codeset_cols, retain_cohort_cols)
	if (length(overlap_cols) > 0) {
		cli_abort(c(
			"x" = "Retained codeset columns and retained cohort columns must be uniquely named.",
			"i" = sprintf("Overlapping column(s): %s", paste(overlap_cols, collapse = ", "))
		))
	}

	missing_codeset_cols <- setdiff(retain_codeset_cols, colnames(codeset))
	if (length(missing_codeset_cols) > 0) {
		cli_abort(c(
			"x" = "Requested retained codeset column(s) were not found.",
			"i" = sprintf("Missing codeset column(s): %s", paste(missing_codeset_cols, collapse = ", "))
		))
	}

	if (length(retain_cohort_cols) > 0 && is.null(cohort)) {
		cli_abort("retain_cohort_cols was supplied, but cohort is NULL.")
	}

	if (!is.null(cohort)) {
		missing_cohort_cols <- setdiff(retain_cohort_cols, colnames(cohort))
		if (length(missing_cohort_cols) > 0) {
			cli_abort(c(
				"x" = "Requested retained cohort column(s) were not found.",
				"i" = sprintf("Missing cohort column(s): %s", paste(missing_cohort_cols, collapse = ", "))
			))
		}
	}

	retain_event_cols <- unique(c(retain_codeset_cols, retain_cohort_cols))
	missing_group_cols <- setdiff(event_group_cols, retain_event_cols)
	if (length(missing_group_cols) > 0) {
		cli_abort(c(
			"x" = "event_group_cols must be retained in the event stream.",
			"i" = "Add them to retain_codeset_cols or retain_cohort_cols.",
			"i" = sprintf("Missing grouped column(s): %s", paste(missing_group_cols, collapse = ", "))
		))
	}

	list(
		retain_codeset_cols = retain_codeset_cols,
		retain_cohort_cols = retain_cohort_cols,
		event_group_cols = event_group_cols,
		retain_event_cols = retain_event_cols
	)
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
) {
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
	multi_table_scope <- match.arg(multi_table_scope)

	retained_col_config <- validate_retained_columns(
		codeset = codeset,
		cohort = cohort,
		retain_codeset_cols = retain_codeset_cols,
		retain_cohort_cols = retain_cohort_cols,
		event_group_cols = event_group_cols
	)

	retain_codeset_cols <- retained_col_config$retain_codeset_cols
	retain_cohort_cols <- retained_col_config$retain_cohort_cols
	event_group_cols <- retained_col_config$event_group_cols

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

	table_name <- codesets$crosswalk %>%
		inner_join(codeset, by = "codetype") %>%
		distinct(table) %>%
		pull()

	if (length(table_name) == 0) {
		cli_abort("No matching tables found for the provided codetypes.")
	}

	if (min_codes_required == 1) {
		message(
			"min_codes_required is set to 1, automatically setting min_days_separation to 0"
		)
		min_days_separation <- 0
	}

	if (length(table_name) > 1) {
		class(codeset) <- c("multi", table_name, class(codeset))
	} else {
		class(codeset) <- c(table_name, class(codeset))
	}

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
		enc_type_fil,
		retain_cols = NULL
) {
	primary_date_col <- table_config$primary_date_column
	fallback_date_col <- table_config$fallback_date_column
	event_code_col <- table_config$code_column

	coalesced_date_col_name <- paste0("criterion_", criterion_suffix, "_date")
	criterion_encounterid <- paste0("encounterid_", criterion_suffix)
	event_code_col_criterion_suffix <- paste0(event_code_col, '_', criterion_suffix)
	criterion_enc_type <- paste0("enc_type_", criterion_suffix)
	retain_cols <- normalize_column_selection(retain_cols)

	start_val <- resolve_date_input(start_date)
	end_val <- resolve_date_input(end_date)

	create_date_filter <- function(data, date_col) {
		conditions <- list()

		if (!is.null(start_val)) {
			conditions <- append(conditions, list(expr(!!sym(date_col) >= !!start_val)))
		}

		if (!is.null(end_val)) {
			conditions <- append(conditions, list(expr(!!sym(date_col) <= !!end_val)))
		}

		if (length(conditions) == 0) {
			return(data)
		}

		if (length(conditions) == 1) {
			filter_expr <- conditions[[1]]
		} else {
			filter_expr <- reduce(conditions, function(x, y) expr(!!x & !!y))
		}

		data %>% filter(!!filter_expr)
	}

	finalize_result <- function(result) {
		keep_cols <- unique(c(
			"patid",
			retain_cols,
			"encounterid",
			event_code_col,
			coalesced_date_col_name,
			if (!is.null(enc_type_fil)) "enc_type"
		))
		keep_cols <- intersect(keep_cols, colnames(result))

		result <- result %>%
			distinct(across(all_of(keep_cols))) %>%
			rename(
				!!sym(criterion_encounterid) := encounterid,
				!!sym(event_code_col_criterion_suffix) := !!sym(event_code_col)
			)

		if ("enc_type" %in% colnames(result)) {
			result <- result %>% rename(!!sym(criterion_enc_type) := enc_type)
		}

		result
	}

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

				result <- create_date_filter(result, coalesced_date_col_name)
				finalize_result(result)
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

		if (!is.null(coalesce_attempt) && distinct_ct(coalesce_attempt, id_col = "patid") > 0) {
			echo_text(sprintf(
				"Successfully coalesced %s and %s",
				primary_date_col,
				fallback_date_col
			))
			return(coalesce_attempt)
		}

		echo_text(sprintf(
			"Coalescing %s and %s yielded no patients, using primary date column only",
			primary_date_col,
			fallback_date_col
		))
	}

	result <- cohort_data %>%
		mutate(!!sym(coalesced_date_col_name) := !!sym(primary_date_col))

	result <- create_date_filter(result, coalesced_date_col_name)
	result <- finalize_result(result) %>% compute_new(indexes = list("patid"))

	result
}

#' Create summary of distinct dates by patient
obtain_first_last_events <- function(
		cohort_data,
		date_col,
		min_codes_required,
		group_cols = NULL
) {
	first_col_name <- paste0("first_", date_col)
	last_col_name <- paste0("last_", date_col)
	group_cols <- unique(c("patid", normalize_column_selection(group_cols)))

	result <- cohort_data %>%
		group_by(across(all_of(group_cols))) %>%
		summarize(
			distinct_dates = n_distinct(!!sym(date_col)),
			!!sym(first_col_name) := min(!!sym(date_col), na.rm = TRUE),
			!!sym(last_col_name) := max(!!sym(date_col), na.rm = TRUE),
			.groups = "drop"
		) %>%
		filter(distinct_dates >= min_codes_required) %>%
		compute_new(., indexes = list("patid"))

	result
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
		enc_type_criterion,
		retain_cols = NULL,
		event_group_cols = NULL
) {
	first_col_name <- paste0("first_", date_col)
	last_col_name <- paste0("last_", date_col)
	retain_cols <- normalize_column_selection(retain_cols)
	event_group_cols <- normalize_column_selection(event_group_cols)
	join_cols <- unique(c("patid", event_group_cols))

	summarized <- summary_data %>%
		inner_join(cohort_data, by = join_cols) %>%
		mutate(
			days_sep_from_first = sql(calc_days_between_dates(first_col_name, date_col)),
			days_sep_from_last = sql(calc_days_between_dates(date_col, last_col_name))
		) %>%
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

	select_cols <- unique(c(
		"patid",
		event_group_cols,
		retain_cols,
		encounterid_criterion,
		enc_type_criterion,
		date_col,
		event_code_criterion
	))

	result <- filter_events(
		summarized,
		qualifying_event = qualifying_event,
		date_col = date_col,
		event_group_cols = event_group_cols
	) %>%
		select(any_of(select_cols)) %>%
		compute_new(., indexes = list("patid"))

	result
}


#'  @export
filter_events <- function(
		summarized,
		qualifying_event,
		date_col,
		event_group_cols = NULL
) {
	group_cols <- unique(c("patid", normalize_column_selection(event_group_cols)))

	if (qualifying_event == "first") {
		result <- summarized %>%
			group_by(across(all_of(group_cols))) %>%
			slice_min(!!sym(date_col), with_ties = FALSE) %>%
			ungroup()
	} else if (qualifying_event == "last") {
		result <- summarized %>%
			group_by(across(all_of(group_cols))) %>%
			slice_max(!!sym(date_col), with_ties = FALSE) %>%
			ungroup()
	} else if (qualifying_event == "random") {
		result <- summarized %>%
			group_by(across(all_of(group_cols))) %>%
			slice_sample(n = 1) %>%
			ungroup()
	} else if (qualifying_event == "all") {
		result <- summarized
	} else {
		stop(sprintf("Unsupported qualifying_event: %s", qualifying_event))
	}

	result
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
) {
	table_config <- get_table_config(codeset)
	table_name <- table_config$table

	message(sprintf("Processing %s table using generic method...", table_name))

	retain_event_cols <- unique(c(
		normalize_column_selection(retain_codeset_cols),
		normalize_column_selection(retain_cohort_cols)
	))

	collected_events <- collect_criteria_events(
		cohort = cohort,
		codeset = codeset,
		start_date = start_date,
		end_date = end_date,
		criterion_suffix = criterion_suffix,
		enc_type_fil = enc_type_fil,
		retain_codeset_cols = retain_codeset_cols,
		retain_cohort_cols = retain_cohort_cols
	)

	final_cohort <- apply_criteria_to_events(
		cohort_data = collected_events,
		start_date = start_date,
		end_date = end_date,
		min_codes_required = min_codes_required,
		min_days_separation = min_days_separation,
		qualifying_event = qualifying_event,
		criterion_suffix = criterion_suffix,
		retain_cols = retain_event_cols,
		event_group_cols = event_group_cols
	)

	validate_final_cohort(final_cohort, table_name)
}


#' Internal branch dispatcher for pre-classed codesets
#' @keywords internal
dispatch_define_criteria_branch <- function(
		cohort = NULL,
		codeset,
		start_date = NULL,
		end_date = NULL,
		min_codes_required = 1,
		min_days_separation = 0,
		qualifying_event = "first",
		criterion_suffix,
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
) {
	dispatch_classes <- setdiff(class(codeset), c("multi", "tbl_df", "tbl", "data.frame"))

	method <- NULL
	for (cls in dispatch_classes) {
		method <- getS3method("define_criteria", cls, optional = TRUE)
		if (!is.null(method)) {
			break
		}
	}

	if (is.null(method)) {
		method <- getS3method("define_criteria", "default", optional = TRUE)
	}

	method(
		cohort = cohort,
		codeset = codeset,
		start_date = start_date,
		end_date = end_date,
		min_codes_required = min_codes_required,
		min_days_separation = min_days_separation,
		qualifying_event = qualifying_event,
		criterion_suffix = criterion_suffix,
		enc_type_fil = enc_type_fil,
		multi_table_scope = multi_table_scope,
		retain_codeset_cols = retain_codeset_cols,
		retain_cohort_cols = retain_cohort_cols,
		event_group_cols = event_group_cols
	)
}

#' Apply qualification rules to a standardized event table
#' @keywords internal
apply_criteria_to_events <- function(
		cohort_data,
		start_date = NULL,
		end_date = NULL,
		min_codes_required = 1,
		min_days_separation = 0,
		qualifying_event = "first",
		criterion_suffix,
		retain_cols = NULL,
		event_group_cols = NULL
) {
	date_col_name <- paste0("criterion_", criterion_suffix, "_date")
	retain_cols <- normalize_column_selection(retain_cols)
	event_group_cols <- normalize_column_selection(event_group_cols)

	if (min_codes_required == 1) {
		final_cohort <- filter_events(
			cohort_data,
			qualifying_event = qualifying_event,
			date_col = date_col_name,
			event_group_cols = event_group_cols
		) %>%
			compute_new(indexes = list("patid"))
	} else {
		distinct_events_summary <- obtain_first_last_events(
			cohort_data = cohort_data,
			date_col = date_col_name,
			min_codes_required = min_codes_required,
			group_cols = event_group_cols
		)

		encounterid_criterion <- paste0("encounterid_", criterion_suffix)
		event_code_criterion <- paste0("event_code_", criterion_suffix)
		enc_type_criterion <- paste0("enc_type_", criterion_suffix)

		final_cohort <- apply_days_separation(
			summary_data = distinct_events_summary,
			cohort_data = cohort_data,
			table_config = NULL,
			date_col = date_col_name,
			start_date = start_date,
			end_date = end_date,
			min_days_separation = min_days_separation,
			qualifying_event = qualifying_event,
			encounterid_criterion = encounterid_criterion,
			event_code_criterion = event_code_criterion,
			enc_type_criterion = enc_type_criterion,
			retain_cols = retain_cols,
			event_group_cols = event_group_cols
		)
	}

	final_cohort
}

#' Collect standardized event-level rows for criteria evaluation
#' @export
collect_criteria_events <- function(
		cohort = NULL,
		codeset,
		start_date = NULL,
		end_date = NULL,
		criterion_suffix,
		enc_type_fil = NULL,
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL
) {
	table_config <- get_table_config(codeset)
	table_name <- table_config$table
	retain_cols <- unique(c(
		normalize_column_selection(retain_codeset_cols),
		normalize_column_selection(retain_cohort_cols)
	))

	input_tbl <- if (!is.null(cohort)) {
		cdm_tbl(table_name) %>% inner_join(cohort, by = "patid")
	} else {
		cdm_tbl(table_name)
	}

	if (.GlobalEnv$cdm_type == "pcornet") {
		if (all(pull(codeset, code) == "*")) {
			if (length(retain_codeset_cols) > 0) {
				cli_abort("retain_codeset_cols is not supported when using wildcard code='*'.")
			}

			echo_text("Wild card request detected")
			codetype_value <- codeset %>% pull(codetype)
			type_value <- codesets$crosswalk %>%
				filter(codetype %in% codetype_value) %>%
				pull(pcornet_vocab_type)

			cohort_tbl <- input_tbl %>%
				filter(!!sym(table_config$type_column) %in% type_value)
		} else {
			cohort_tbl <- input_tbl %>%
				inner_join(codeset, by = setNames("code", table_config$code_column))
		}
	} else {
		cohort_tbl <- input_tbl %>%
			inner_join(codeset, by = setNames("concept_id", table_config$code_column))
	}

	if (.GlobalEnv$cdm_type == "pcornet" && !is.null(enc_type_fil)) {
		if (table_name %in% c("diagnosis", "procedures")) {
			cohort_tbl <- cohort_tbl %>% filter(enc_type %in% enc_type_fil)
		} else {
			cohort_tbl <- cohort_tbl %>%
				inner_join(
					cdm_tbl("encounter") %>% select(patid, encounterid, enc_type)
				) %>%
				filter(enc_type %in% enc_type_fil)
		}
	}

	filtered_tbl <- apply_date_filters(
		cohort_data = cohort_tbl,
		table_config = table_config,
		start_date = start_date,
		end_date = end_date,
		criterion_suffix = criterion_suffix,
		enc_type_fil = enc_type_fil,
		retain_cols = retain_cols
	)

	encounter_col <- paste0("encounterid_", criterion_suffix)
	date_col <- paste0("criterion_", criterion_suffix, "_date")
	enc_type_col <- paste0("enc_type_", criterion_suffix)
	code_col <- paste0(table_config$code_column, "_", criterion_suffix)
	event_code_col <- paste0("event_code_", criterion_suffix)

	select_cols <- unique(c(
		"patid",
		retain_cols,
		encounter_col,
		date_col,
		if (enc_type_col %in% colnames(filtered_tbl)) enc_type_col,
		event_code_col
	))

	result <- filtered_tbl %>%
		rename(!!sym(event_code_col) := !!sym(code_col))

	result %>%
		select(any_of(select_cols)) %>%
		compute_new(
			name = glue("collected_{criterion_suffix}_{table_name}"),
			indexes = list("patid")
		)
}

#' Multi-table S3 method that delegates to table-specific methods and unions results
#' @export
define_criteria.multi <- function(
		cohort = NULL,
		codeset,
		start_date = NULL,
		end_date = NULL,
		min_codes_required = 1,
		min_days_separation = 0,
		qualifying_event = "first",
		criterion_suffix,
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
) {
	multi_table_scope <- match.arg(multi_table_scope)

	tables_to_query <- .GlobalEnv$codesets$crosswalk %>%
		inner_join(codeset, by = "codetype") %>%
		distinct(table) %>%
		pull()

	if (length(tables_to_query) == 0) {
		cli_abort("No matching tables found for the provided codetypes.")
	}

	echo_text(sprintf(
		"Processing tables: %s using multi method (%s scope)...",
		paste(tables_to_query, collapse = ", "),
		multi_table_scope
	))

	encounter_col <- paste0("encounterid_", criterion_suffix)
	date_col <- paste0("criterion_", criterion_suffix, "_date")
	enc_type_col <- paste0("enc_type_", criterion_suffix)
	event_code_col <- paste0("event_code_", criterion_suffix)
	retain_event_cols <- unique(c(
		normalize_column_selection(retain_codeset_cols),
		normalize_column_selection(retain_cohort_cols)
	))

	normalize_result <- function(result) {
		result_cols <- colnames(result)

		if (!(encounter_col %in% result_cols)) {
			result <- result %>% mutate(!!sym(encounter_col) := NA_character_)
			result_cols <- c(result_cols, encounter_col)
		}

		if (!(date_col %in% result_cols)) {
			cli_abort(sprintf(
				"Result is missing expected date column '%s' required for multi-table union.",
				date_col
			))
		}

		if (!(event_code_col %in% result_cols)) {
			protected_cols <- c("patid", retain_event_cols, encounter_col, date_col, enc_type_col)
			candidate_code_cols <- setdiff(result_cols, protected_cols)
			candidate_code_cols <- candidate_code_cols[grepl(paste0("_", criterion_suffix, "$"), candidate_code_cols)]

			if (length(candidate_code_cols) >= 1) {
				result <- result %>% rename(!!sym(event_code_col) := !!sym(candidate_code_cols[1]))
			} else {
				result <- result %>% mutate(!!sym(event_code_col) := NA_character_)
			}
		}

		select_cols <- unique(c(
			"patid",
			retain_event_cols,
			encounter_col,
			date_col,
			if (enc_type_col %in% colnames(result)) enc_type_col,
			event_code_col
		))

		result %>%
			select(any_of(select_cols))
	}

	event_list <- tables_to_query %>%
		purrr::map(function(tbl_name) {
			sub_codeset <- codeset %>%
				semi_join(
					.GlobalEnv$codesets$crosswalk %>%
						filter(table == tbl_name) %>%
						distinct(codetype),
					by = "codetype"
				)

			collect_criteria_events(
				cohort = cohort,
				codeset = sub_codeset,
				start_date = start_date,
				end_date = end_date,
				criterion_suffix = criterion_suffix,
				enc_type_fil = enc_type_fil,
				retain_codeset_cols = retain_codeset_cols,
				retain_cohort_cols = retain_cohort_cols
			) %>%
				normalize_result() %>%
				compute_new(indexes = list("patid"))
		})

	pooled_events <- purrr::reduce(event_list, dplyr::union_all) %>%
		compute_new(
			name = glue("combined_{criterion_suffix}_events"),
			indexes = list("patid")
		)

	if (multi_table_scope == "post_union") {
		final_cohort <- apply_criteria_to_events(
			cohort_data = pooled_events,
			start_date = start_date,
			end_date = end_date,
			min_codes_required = min_codes_required,
			min_days_separation = min_days_separation,
			qualifying_event = qualifying_event,
			criterion_suffix = criterion_suffix,
			retain_cols = retain_event_cols,
			event_group_cols = event_group_cols
		)

		return(validate_final_cohort(final_cohort, paste(tables_to_query, collapse = "+")))
	}

	qualified_list <- event_list %>%
		purrr::map(function(event_tbl) {
			apply_criteria_to_events(
				cohort_data = event_tbl,
				start_date = start_date,
				end_date = end_date,
				min_codes_required = min_codes_required,
				min_days_separation = min_days_separation,
				qualifying_event = qualifying_event,
				criterion_suffix = criterion_suffix,
				retain_cols = retain_event_cols,
				event_group_cols = event_group_cols
			)
		})

	per_table_result <- purrr::reduce(qualified_list, dplyr::union_all) %>%
		compute_new(
			name = glue("combined_{criterion_suffix}_per_table"),
			indexes = list("patid")
		)

	if (multi_table_scope == "per_table") {
		return(validate_final_cohort(per_table_result, paste(tables_to_query, collapse = "+")))
	}

	post_union_result <- apply_criteria_to_events(
		cohort_data = pooled_events,
		start_date = start_date,
		end_date = end_date,
		min_codes_required = min_codes_required,
		min_days_separation = min_days_separation,
		qualifying_event = qualifying_event,
		criterion_suffix = criterion_suffix,
		retain_cols = retain_event_cols,
		event_group_cols = event_group_cols
	)

	list(
		post_union = validate_final_cohort(post_union_result, paste(tables_to_query, collapse = "+")),
		per_table = validate_final_cohort(per_table_result, paste(tables_to_query, collapse = "+"))
	)
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
	)
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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
		enc_type_fil,
		multi_table_scope,
		retain_codeset_cols,
		retain_cohort_cols,
		event_group_cols
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
		enc_type_fil = NULL,
		multi_table_scope = c("post_union", "per_table", "both"),
		retain_codeset_cols = NULL,
		retain_cohort_cols = NULL,
		event_group_cols = NULL
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