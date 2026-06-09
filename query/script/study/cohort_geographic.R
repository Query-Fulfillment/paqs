#' Retrieve geographic information for a cohort
#'
#' Joins the `lds_address_history` table to the provided cohort (if any) and returns the most recent address per patient, handling fallbacks for missing ZIPs.
#'
#' @param cohort Optional cohort table to join on `patid`.
#' @param end_date End date for the look‑back period (default "2024-12-31").
#' @param lookback_years Number of years to look back from `end_date` (default 5).
#' @return A validated cohort table with the latest address fields.
#' @examples
#' get_geog_info(cohort = my_cohort)
get_geog_info <- function(
		cohort = NULL,
		end_date = "2025-12-31",
		lookback_years = 5
) {
	message("Starting get_geog_info()...")
	table_name <- "lds_address_history"

	# Join with cohort if supplied
	input_tbl <- if (!is.null(cohort)) {
		echo_text("Joining lds_address_history table with supplied cohort")
		cdm_tbl(table_name) %>% inner_join(cohort, by = "patid") %>%
			compute_new(indexes = list("patid"))
	} else {
		echo_text("No cohort supplied, using full lds_address_history table")
		cdm_tbl(table_name)
	}

	# Resolve date inputs
	#start_date_resolved <- resolve_date_input(start_date)
	end_date_resolved   <- resolve_date_input(end_date)

	# Bad value definitions
	bad_zip   <- c("99999", "00000", "Missing")
	bad_state <- c("NI", "Missing", "YY", "ZZ", "UN")

	# Lookback
	eqp_end <- if (!is.null(end_date_resolved)) as.Date(end_date_resolved) else Sys.Date()
	lookback_date <- eqp_end %m-% years(lookback_years)

	# Rank addresses by recency
	echo_text("Rank addresses by recency")
	all_addresses <- input_tbl %>%
		filter(!is.na(address_zip5),
					 !address_zip5 %in% bad_zip,
					 !is.na(address_state),
					 !address_state %in% bad_state,
					 (is.na(address_period_end) | address_period_end > lookback_date),
					 !is.na(address_period_start)) %>%
		mutate(current_flag = if_else((!is.na(address_period_start) & is.na(address_period_end)), 1L, 0L)) %>%
		window_order(patid, desc(current_flag), desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting the most recent address")
	top_address <- all_addresses %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	# Case where address_period_start is missing but current_address_flag = Y
	echo_text("Get addresses where current_address_flag = Y")
	current_addresses <- input_tbl %>%
		anti_join(select(top_address, patid), by = 'patid') %>%
		filter(!is.na(address_zip5),
					 !address_zip5 %in% bad_zip,
					 !is.na(address_state),
					 !address_state %in% bad_state,
					 current_address_flag == 'Y') %>%
		window_order(patid, desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting current address")
	top_current_address <- current_addresses %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	# Fallbacks
	echo_text("Identifying fallback ZIPs...")
	valid_zip <- input_tbl %>%
		anti_join(select(top_address, patid), by = 'patid') %>%
		anti_join(select(top_current_address, patid), by = 'patid') %>%
		filter(!is.na(address_zip5),
					 !address_zip5 %in% bad_zip,
					 (is.na(address_period_end) | address_period_end > lookback_date),
					 !is.na(address_period_start)) %>%
		mutate(current_flag = if_else((!is.na(address_period_start) & is.na(address_period_end)), 1L, 0L)) %>%
		window_order(patid, desc(current_flag), desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting the most recent address for fallback ZIPs")
	top_address_zip <- valid_zip %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	echo_text("Identifying fallback states...")
	valid_state <- input_tbl %>%
		anti_join(select(top_address, patid), by = 'patid') %>%
		anti_join(select(top_current_address, patid), by = 'patid') %>%
		anti_join(select(top_address_zip, patid), by = 'patid') %>%
		filter(!is.na(address_state),
					 !address_state %in% bad_state,
					 (is.na(address_period_end) | address_period_end > lookback_date),
					 !is.na(address_period_start)) %>%
		mutate(current_flag = if_else((!is.na(address_period_start) & is.na(address_period_end)), 1L, 0L)) %>%
		window_order(patid, desc(current_flag), desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting the most recent address for fallback states")
	top_address_state <- valid_state %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	echo_text("Get addresses where current_address_flag = Y and nonmissing zip")
	current_addresses_zip <- input_tbl %>%
		anti_join(select(top_address, patid), by = 'patid') %>%
		anti_join(select(top_current_address, patid), by = 'patid') %>%
		anti_join(select(top_address_zip, patid), by = 'patid') %>%
		anti_join(select(top_address_state, patid), by = 'patid') %>%
		filter(!is.na(address_zip5),
					 !address_zip5 %in% bad_zip,
					 current_address_flag == 'Y') %>%
		window_order(patid, desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting current address with zip")
	top_current_address_zip <- current_addresses_zip %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	echo_text("Get addresses where current_address_flag = Y and nonmissing state")
	current_addresses_state <- input_tbl %>%
		anti_join(select(top_address, patid), by = 'patid') %>%
		anti_join(select(top_current_address, patid), by = 'patid') %>%
		anti_join(select(top_address_zip, patid), by = 'patid') %>%
		anti_join(select(top_address_state, patid), by = 'patid') %>%
		anti_join(select(top_current_address_zip, patid), by = 'patid') %>%
		filter(!is.na(address_state),
					 !address_state %in% bad_state,
					 current_address_flag == 'Y') %>%
		window_order(patid, desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting current address")
	top_current_address_state <- current_addresses_state %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	# Final addresses
	final_addresses <- top_address %>%
		union_all(top_current_address) %>%
		union_all(top_address_zip) %>%
		union_all(top_address_state) %>%
		union_all(top_current_address_zip) %>%
		union_all(top_current_address_state) %>%
		distinct(patid, address_zip5, address_state, address_period_end, address_period_start, addressid) %>%
		compute_new(indexes = list("patid"))


	return(validate_final_cohort(final_addresses, table_name))
}

#' Map geographic variables to address data
#'
#' Joins the appropriate mapping table (ADI, RUCA, or state) to an address table.
#'
#' @param address_tbl Address table containing `patid` and location identifiers.
#' @param geo_var One of "adi", "ruca", or "state" indicating which mapping to apply.
#' @return A table with `patid` and the mapped geographic variable.
#' @examples
#' map_geographic_variables(address_tbl, geo_var = "adi")
map_geographic_variables <- function(
		address_tbl,
		geo_var = c("adi", "ruca", "state")
) {

	echo_text("Starting map_geographic_variables()...")

	# Load mapping files only as needed
	if (geo_var == "adi") {
		ses_map <- load_codeset('ses_map', col_types = "cccc", indexes = NULL)
		echo_text("Getting ADI")
		mapped_tbl <- address_tbl %>%
			left_join(
				ses_map %>%
					select(address_zip5 = CDM_Value, adi_quartile = MAP_Value),
				by = c("address_zip5")
			) %>%
			select(patid, adi_quartile) %>%
			compute_new(indexes = list("patid"))
	}

	if (geo_var == "ruca") {
		ruca_map <- load_codeset('ruca_map', col_types = "cccc", indexes = NULL)
		echo_text("Getting RUCA")
		mapped_tbl <- address_tbl %>%
			left_join(
				ruca_map %>%
					select(address_zip5 = CDM_Value, ruca_code = MAP_Value),
				by = c("address_zip5")
			) %>%
			select(patid, ruca_code) %>%
			compute_new(indexes = list("patid"))
	}

	if (geo_var == "state") {
		state_map <- load_codeset('state_map', col_types = "cccc", indexes = NULL)
		echo_text("Getting state")
		mapped_tbl <- address_tbl %>%
			left_join(
				state_map %>%
					select(address_state = CDM_Value, state_name = MAP_Value, us_census_region = US_Census_Region),
				by = c("address_state")
			) %>%
			select(patid, state_name, us_census_region) %>%
			compute_new(indexes = list("patid"))
	}

	return(mapped_tbl)
}
#' Validate geographic data completeness
#'
#' Flags valid ZIP and state values for a cohort.
#'
#' @param cohort Cohort table containing address fields.
#' @return A table with boolean flags for ZIP and state validity.
#'
#' @examples
#' get_valid_geog(cohort)
get_valid_geog <- function(
		cohort = NULL) {

	message("Starting get_valid_geog()...")

	valid_geog <- cohort %>%
		mutate(valid_zip = if_else(address_zip5 != "99999" &
							 address_zip5 != "00000" &
							 address_zip5 != "Missing" & !is.na(address_zip5), 'Yes', NA)) %>%
		mutate(valid_state = if_else(address_state != "NI" &
							 address_state != "Missing" &
							 address_state != "YY" &
							 address_state != "ZZ" &
							 address_state != "UN" & !is.na(address_state), 'Yes', NA)) %>%
		mutate(valid_zip_or_state = if_else(valid_zip == 'Yes' | valid_state == 'Yes', 'Yes', NA)) %>%
		mutate(valid_zip_and_state = if_else(valid_zip == 'Yes' & valid_state == 'Yes', 'Yes', NA)) %>%
		select(patid, valid_zip, valid_state, valid_zip_or_state, valid_zip_and_state)

	return(valid_geog)
}
