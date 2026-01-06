get_geog_info <- function(
		cohort = NULL,
		end_date = "2024-12-31",
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
		mutate(recency_flag = if_else((!is.na(address_period_start) & is.na(address_period_end)), 1L, 0L)) %>%
		window_order(patid, desc(recency_flag), desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting the most recent address")
	top_address <- all_addresses %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	# Fallbacks
	echo_text("Identifying fallback ZIPs...")
	valid_zip <- input_tbl %>%
		anti_join(select(top_address, patid), by = 'patid') %>%
		filter(!is.na(address_zip5),
					 !address_zip5 %in% bad_zip,
					 (is.na(address_period_end) | address_period_end > lookback_date),
					 !is.na(address_period_start)) %>%
		mutate(recency_flag = if_else((!is.na(address_period_start) & is.na(address_period_end)), 1L, 0L)) %>%
		window_order(patid, desc(recency_flag), desc(address_period_end),
								 desc(address_period_start), desc(addressid)) %>%
		group_by(patid) %>%
		mutate(recency_rank = row_number()) %>%
		ungroup() %>%
		compute_new(indexes = list("patid"))

	echo_text("Selecting the most recent address for fallback ZIPs")
	top_address_zip <- valid_zip %>% filter(recency_rank == 1) %>%
		compute_new(indexes = list("patid"))

	# Final addresses
	final_addresses <- top_address %>%
		union_all(top_address_zip) %>%
		distinct(patid, address_zip5, address_state, address_period_end, address_period_start, addressid) %>%
		compute_new(indexes = list("patid"))


	return(validate_final_cohort(final_addresses, table_name))
}


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
					select(address_state = CDM_Value, state_name = MAP_Value),
				by = c("address_state")
			) %>%
			select(patid, state_name) %>%
			compute_new(indexes = list("patid"))
	}

	return(mapped_tbl)
}