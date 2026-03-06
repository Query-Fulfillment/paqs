#' Define payer type criteria
#'
#' Summarizes primary payer types for each patient within a cohort and assigns category ranks.
#'
get_payer_info <- function(
		cohort,
		cohort_encounterid_col
) {

	echo_text("Starting get_payer_info...")
	table_name <- "encounter"

	payer_map <- load_codeset('payer_type_primary_map', col_types = "ccc",indexes = NULL)


	# Join encounters for the cohort ----
	echo_text("Step 1: Joining encounter table with supplied cohort")
	cohort_enc <- cohort %>%
		mutate(encounterid = !!sym(cohort_encounterid_col)) %>%
		inner_join(
			cdm_tbl(table_name) %>%
				select(patid, encounterid, payer_type_primary, payer_type_secondary, admit_date),
			by = c("patid", "encounterid")
		) %>%
		compute_new(indexes = list("patid"))


	# Map to payer type PRIMARY and assign ranking ----

	echo_text("Step 2: Mapping to payer type and choose most recent visit with non-missing primary or secondary payer type")
	payer_tbl <- cohort_enc %>%
		select(patid, encounterid, payer_type_primary, payer_type_secondary, admit_date) %>%
		mutate(payer_type_primary = as.character(payer_type_primary),
					 payer_type_secondary = as.character(payer_type_secondary)) %>%
		left_join(payer_map, by = c("payer_type_primary" = "code")) %>%
		left_join(payer_map, by = c("payer_type_secondary" = "code"), suffix = c("_primary", "_secondary")) %>%
		#filter(!payer_type_cat == "Missing") %>%
		mutate(
			primary_payer_cat_rank = case_when(
				payer_type_cat_primary %in% c("Medicare", "Medicaid") ~ 1L,
				payer_type_cat_primary == "Private" ~ 2L,
				payer_type_cat_primary == "Other" ~ 3L,
				TRUE ~ 4L
			),
			secondary_payer_cat_rank = case_when(
				payer_type_cat_secondary %in% c("Medicare", "Medicaid") ~ 1L,
				payer_type_cat_secondary == "Private" ~ 2L,
				payer_type_cat_secondary == "Other" ~ 3L,
				TRUE ~ 4L
			)
		) %>%
		filter(primary_payer_cat_rank != 4L | secondary_payer_cat_rank != 4L) %>%
		group_by(patid) %>%
		window_order(desc(admit_date), encounterid) %>%
		distinct(patid, .keep_all = TRUE) %>%
		ungroup() %>%
		mutate(payer_cat_rank = case_when(
			primary_payer_cat_rank<secondary_payer_cat_rank ~ primary_payer_cat_rank,
			secondary_payer_cat_rank<primary_payer_cat_rank ~ secondary_payer_cat_rank),
			payer_type_cat = case_when(
				primary_payer_cat_rank < secondary_payer_cat_rank ~ payer_type_cat_primary,
				secondary_payer_cat_rank < primary_payer_cat_rank ~ payer_type_cat_secondary,
				TRUE ~ payer_type_cat_primary)
		) %>%
		select(patid, encounterid_payer = encounterid, payer_date = admit_date,
					 payer_rank = payer_cat_rank, payer_cat = payer_type_cat) %>%
		compute_new(indexes = list("patid"))


	return(validate_final_cohort(payer_tbl, table_name))
}