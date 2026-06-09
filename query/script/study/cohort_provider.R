#' Get provider types
#'
#' Summarizes provider specialties for a list of encounters
#'
#' @param cohort table with patid and encounterid
#' @param providers provider specialty codeset name
#' @param cohort_encounterid_col name of encounterid from cohort table
get_provider_info <- function(
		cohort,
		providers,
		cohort_encounterid_col) {

	echo_text("Starting get_provider_info...")
	table_name_1 <- "encounter"
	table_name_2 <- "provider"

	# Join encounters for the cohort ----
	echo_text("Step 1: Joining encounter table with supplied cohort")
	cohort_enc <- cohort %>%
		mutate(encounterid = !!sym(cohort_encounterid_col)) %>%
		inner_join(
			cdm_tbl(table_name_1) %>%
				select(patid, encounterid, providerid, admit_date),
			by = c("patid", "encounterid")
		) %>%
		compute_new(indexes = list("patid"))

	echo_text("Step 2: Joining to provider table and getting specific specialty")


	subset_codesets <- codesets[providers]
	all_providers <- list()
	provider_rslt <- cohort_enc %>% distinct(patid)

	for (name in names(subset_codesets)) {

		prov <- subset_codesets[[name]]

		cohort_provider <- cohort_enc %>%
			inner_join(cdm_tbl(table_name_2), by = c('providerid')) %>%
			inner_join(prov, by = c('provider_specialty_primary' = 'code')) %>%
			distinct(patid, encounterid_provider = encounterid, providerid, provider_specialty_primary,
							 provider_date = admit_date) %>%
			compute_new(indexes = list("patid"))

		prov_flag <- cohort_provider %>%
			distinct(patid) %>%
			mutate(!!name := "yes")

		provider_rslt <- provider_rslt %>%
			left_join(prov_flag, by = 'patid') %>%
			compute_new(indexes = list('patid'))
	}
	provider_rslt
}