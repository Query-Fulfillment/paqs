load_all_codesets <- function(path = paste0('./query/',get_argos_default()$config('subdirs')$spec_dir)) {

	files <- gsub(".csv","",list.files(path, pattern = 'csv'))

	exclude_files <- c("ses_map", "ruca_map", "state_map", "payer_type_primary_map", "cohort_names_map", "crosswalks","crosswalk_omop")
	files <- setdiff(files, exclude_files)

	if(.GlobalEnv$cdm_type == "pcornet") {
	codesets <- setNames(
		lapply(files, load_codeset, col_types = "ccc", indexes = "code"),
		basename(files)
	)
} else {
	codesets <- setNames(
		lapply(files, load_codeset, col_types = "iccc"),
		basename(files)
	)
	}

	codesets[["crosswalk"]] <- load_codeset('crosswalks', 'ccc', indexes = NULL)
	codesets[["crosswalk_omop"]] <- load_codeset('crosswalk_omop', 'ccc', indexes = NULL)
	codesets
	}