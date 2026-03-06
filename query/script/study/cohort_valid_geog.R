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