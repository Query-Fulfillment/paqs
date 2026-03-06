#'
#' This file contains functions to identify cohorts in a study.  Its
#' contents, as well as the contents of any R files whose name begins
#' with "cohort_", will be automatically sourced when the request is
#' executed.
#'
#' For simpler requests, it will often be possible to do all cohort
#' manipulation in functions within this file.  For more complex
#' requests, it may be more readable to separate different cohorts or
#' operations on cohorts into logical groups in different files.
#'
#'#'
#' This file contains functions to identify cohorts in a study.  Its
#' contents, as well as the contents of any R files whose name begins
#' with "cohort_", will be automatically sourced when the request is
#' executed.
#'
#' For simpler requests, it will often be possible to do all cohort
#' manipulation in functions within this file.  For more complex
#' requests, it may be more readable to separate different cohorts or
#' operations on cohorts into logical groups in different files.
#

#' Generate one-way table one for set of variables
#'
#' @param cohort cohort of interest
#' @param cat_vars vector of categorical variables
#' @param cont_var vector of continuous variables
#'
#' @return
#' @export
#'
#' @examples
#'
get_table_one <- function(cohort, cat_vars = NULL, cont_var = NULL) {
	result <- list()

	if (
		is.null(cat_vars) &
			is.null(cont_var)
	) {
		stop(
			"Table one needs alteast one continuous or a categorical variable specified"
		)
	}

	if (!is.null(cat_vars)) {
		for (i in cat_vars) {
			if (!any(colnames(cohort) == i)) {
				next
			}
			result[[i]] <- cohort %>%
				count(!!sym(i)) %>%
				mutate(
					!!sym(i) := as.character(!!sym(i)),
					variable = paste0(i, "_", !!sym(i)),
					block = i,
					var_type = "cat",
					n = as.numeric(n)
				) %>%

				rename(
					Characteristics = !!sym(i),
					value = n
				) %>%
				# mutate(
				# 	Characteristics = as.character(Characteristics)  # <-- cast as character
				# ) %>%
				collect_new()
		}
	}

	if (!is.null(cont_var)) {
		if (
			!any(
				class(get_argos_default()$config("db_src")) ==
					c("PqConnection", "duckdb_connection")
			)
		) {
			for (i in cont_var) {
				if (!any(colnames(cohort) == i)) {
					next
				}
				result[[i]] <- cohort %>%
					mutate(
						mean = as.numeric(mean(!!sym(i), na.rm = T)),
						sd = as.numeric(sd(!!sym(i), na.rm = T)),
						median = as.numeric(median(!!sym(i))),
						quantile_25 = as.numeric(quantile(!!sym(i), 0.25)),
						quantile_75 = as.numeric(quantile(!!sym(i), 0.75)),
						block = i,
						var_type = "cont"
					) %>%
					distinct(
						mean,
						sd,
						median,
						quantile_25,
						quantile_75,
						block,
						var_type
					) %>%
					tidyr::pivot_longer(
						cols = c("mean", "median", "sd", "quantile_25", "quantile_75"),
						values_to = "value"
					) %>%
					mutate(
						variable = paste0(i, "_", name),
						block = i,
						var_type = "cont"
					) %>%
					rename(Characteristics = name) %>%
					collect_new()
			}
		} else {
			for (i in cont_var) {
				if (!any(colnames(cohort) == i)) {
					next
				}
				result[[i]] <- cohort %>%
					summarise(
						mean = as.numeric(mean(!!sym(i), na.rm = T)),
						sd = as.numeric(sd(!!sym(i), na.rm = T)),
						median = as.numeric(median(!!sym(i))),
						quantile_25 = as.numeric(quantile(!!sym(i), 0.25)),
						quantile_75 = as.numeric(quantile(!!sym(i), 0.75)),
						block = i,
						var_type = "cont"
					) %>%
					tidyr::pivot_longer(
						cols = c("mean", "median", "sd", "quantile_25", "quantile_75"),
						values_to = "value"
					) %>%
					mutate(variable = paste0(block, name)) %>%
					mutate(name = paste0(block, " ", name)) %>%
					rename(Characteristics = name) %>%
					collect_new()
			}
		}
	}

	result[["total"]] <- cohort %>%
		count() %>%
		mutate(
			block = "Total",
			variable = "Total",
			Characteristics = "Total",
			var_type = "Total"
		) %>%
		mutate(n = as.numeric(n)) %>%
		rename(value = n) %>%
		collect_new()

	table_1 <- reduce(result, union_all) %>%
		select(block, variable, Characteristics, var_type, value)

	block_order <- c(
		"Total",
		"age_at_end",
		cat_vars
	)

	table_1_ordered <- table_1 %>%
		mutate(
			block = factor(
				block,
				levels = c(block_order, setdiff(unique(block), block_order))
			)
		) %>%
		arrange(block)

	return(table_1_ordered)
}

#' @export
get_table_one_new <- function(cohort, cat_vars = NULL, cont_var = NULL) {
	# Validate inputs
	if (is.null(cat_vars) && is.null(cont_var)) {
		stop(
			"Table one needs at least one continuous or a categorical variable specified"
		)
	}

	# Determine which columns to collect
	vars_to_collect <- c(cat_vars, cont_var)
	vars_to_collect <- vars_to_collect[vars_to_collect %in% colnames(cohort)]

	if (length(vars_to_collect) == 0) {
		stop("None of the specified variables exist in the cohort")
	}

	# Collect data once upfront
	cohort_data <- cohort %>%
		select(all_of(vars_to_collect)) %>%
		collect_new()

	# Process categorical variables
	cat_results <- NULL
	if (!is.null(cat_vars)) {
		cat_vars_present <- cat_vars[cat_vars %in% colnames(cohort_data)]

		if (length(cat_vars_present) > 0) {
			cat_results <- map_dfr(cat_vars_present, function(i) {
				cohort_data %>%
					count(!!sym(i), .drop = FALSE) %>%
					mutate(
						!!sym(i) := as.character(!!sym(i)),
						variable = paste0(i, "_", !!sym(i)),
						block = i,
						var_type = "cat",
						n = as.numeric(n)
					) %>%
					rename(
						Characteristics = !!sym(i),
						value = n
					) %>%
					select(block, variable, Characteristics, var_type, value)
			})
		}
	}

	# Process continuous variables
	cont_results <- NULL
	if (!is.null(cont_var)) {
		cont_var_present <- cont_var[cont_var %in% colnames(cohort_data)]

		if (length(cont_var_present) > 0) {
			cont_results <- map_dfr(cont_var_present, function(i) {
				var_data <- cohort_data[[i]]

				tibble(
					mean = as.numeric(mean(var_data, na.rm = TRUE)),
					sd = as.numeric(sd(var_data, na.rm = TRUE)),
					median = as.numeric(median(var_data, na.rm = TRUE)),
					quantile_25 = as.numeric(quantile(var_data, 0.25, na.rm = TRUE)),
					quantile_75 = as.numeric(quantile(var_data, 0.75, na.rm = TRUE))
				) %>%
					tidyr::pivot_longer(
						cols = everything(),
						names_to = "Characteristics",
						values_to = "value"
					) %>%
					mutate(
						variable = paste0(i, "_", Characteristics),
						block = i,
						var_type = "cont",
						value = as.numeric(value)
					) %>%
					select(block, variable, Characteristics, var_type, value)
			})
		}
	}

	# Add total count
	total_result <- tibble(
		block = "Total",
		variable = "Total",
		Characteristics = "Total",
		var_type = "Total",
		value = as.numeric(nrow(cohort_data))
	)

	# Combine all results
	table_1 <- bind_rows(cat_results, cont_results, total_result)

	# Create ordered factor for block
	block_order <- c(
		"Total",
		"age_at_end",
		cat_vars
	)

	table_1_ordered <- table_1 %>%
		mutate(
			block = factor(
				block,
				levels = c(block_order, setdiff(unique(block), block_order))
			)
		) %>%
		arrange(block)

	rm(cohort_data)
	gc()

	return(table_1_ordered)
}


in_memory_bash_cli_library <- function() {
	script <- '
#!/usr/bin/env bash

# A minimal Bash CLI styling library
BOLD="\\033[1m"
RESET="\\033[0m"
RED="\\033[31m"
GREEN="\\033[32m"
YELLOW="\\033[33m"
BLUE="\\033[34m"

cli_info() {
  echo -e "${BLUE}ℹ${RESET} $1"
}

cli_success() {
  echo -e "${GREEN}✔${RESET} $1"
}

cli_warning() {
  echo -e "${YELLOW}!${RESET} $1"
}

cli_error() {
  echo -e "${RED}✖${RESET} $1"
}

cli_heading() {
  echo -e "${BOLD}$1${RESET}"
  echo -e "${BOLD}----------------------------------------${RESET}"
}
'
	script
}


#' Function to echo terminal message while running a docker container with R in terminal
#'
#' @param text String of message to be generated. Dynamic object can be passed with paste0()
#'
#' @return
#' @export
#'
#' @examples
echo_text <- function(text, style = "info") {
	if (.Platform$OS.type == "windows") {
		sys.echo <-
			paste0("system('echo ", text, "')")

		source(textConnection(sys.echo))
	} else {
		script <- in_memory_bash_cli_library()

		bash_styles <- c(
			info = "cli_info",
			success = "cli_success",
			warning = "cli_warning",
			error = "cli_error",
			heading = "cli_heading"
		)

		if (!style %in% names(bash_styles)) {
			stop(
				"Unknown style '",
				style,
				"'. Valid styles are: ",
				paste(names(bash_styles), collapse = ", "),
				"."
			)
		}

		bash_function <- bash_styles[[style]]

		cmd <- sprintf(
			"bash << 'EOF'\n%s\n%s \"%s\"\nEOF",
			script,
			bash_function,
			text
		)

		system(cmd)
	}
	r_styles <- list(
		info = cli::cli_alert_info,
		success = cli::cli_alert_success,
		warning = cli::cli_alert_warning,
		error = cli::cli_alert_danger,
		heading = function(txt) cli::cli_h1(txt)
	)

	r_styles[[style]](text)
}

#' @export
show_progress <- function(step, message) {
	progress_pct <- round(step / .GlobalEnv$total_steps * 100, 0)

	if (.Platform$OS.type == "windows") {
		text <- paste0("PROGRESS: ", progress_pct, "%\n")

		sys.echo <-
			paste0("system('echo ", text, "')")

		source(textConnection(sys.echo))
	} else {
		echo_text(paste0("PROGRESS: ", progress_pct, "%\n"))
	}

	cli::cli_h1(paste0("Step ", step, "/", total_steps, ": ", message))
}


#' Function to sink into log file
#'
#' @returns
#'
#' @export
#' @examples
start_log <- function() {
	if (Sys.getenv('execution_mode') %in% c('container', 'nativeR')) {
		.GlobalEnv$logFile <- file(
			file.path(
				get_argos_default()$config("base_dir"),
				get_argos_default()$config("subdirs")$result_dir,
				paste0(.GlobalEnv$query_name, ".log")
			),
			open = "wt"
		)

		sink(file = .GlobalEnv$logFile,
				 type = "message",
				 append = TRUE)
		sink(file = .GlobalEnv$logFile,
				 type = "output",
				 append = TRUE)
	}

	.GlobalEnv$query_start_time <- Sys.time()
}

#' Function to sink out of log file
#'
#' @returns
#'
#' @export
#' @examples
end_log <- function() {
	if (Sys.getenv('execution_mode') %in% c('container', 'nativeR')) {
		sink(type = "output")
		sink(type = "message")
		close.connection(.GlobalEnv$logFile)

		.GlobalEnv$query_end_time <- Sys.time()
	}
	signature <-
		tibble(
			`Query Start Time` = .GlobalEnv$query_start_time,
			`Query End Time` = .GlobalEnv$query_end_time,
			`Total Run Time` = difftime(
				.GlobalEnv$query_end_time,
				.GlobalEnv$query_start_time,
				units = "mins"
			)
		)
	output_tbl(signature, name = paste0('signature'))
}


#' Function to start rendering an html file
#'
#' @returns
#'
#' @export
#' @examples
render_report <- function() {
	if (Sys.getenv("native_execution") != "") {
		if (!as.logical(Sys.getenv("native_execution"))) {
			system(
				"quarto render query/script/report.qmd --output-dir ../results/ --execute-dir query/results/ --to html"
			)
		} else {
			quarto::quarto_render(
				"query/script/report.qmd",
				execute_dir = "query/results/"
			)
			file.rename(
				paste0("query/script/", .GlobalEnv$query_name, "_report.html"),
				paste0("query/results/", .GlobalEnv$query_name, "_report.html")
			)
		}
	} else {
		system(
			"quarto render query/script/report.qmd --output-dir ../results/ --execute-dir query/results/ --to html"
		)
	}
}

#' Function to get sql code for number of days between date1 and date2. Adapted for sql dialects for Postgres and MS SQL.
#' Should always be wrapped by sql()
#' @param date_col_1 Date col 1
#' @param date_col_2 Date col 2
#' @param db connection type object. Defaulted to config('db_src') for standard framework
#' Functionality added for Postgres, MS SQL and Snowflake
#'
#' @return
#' @export
#'
#' @examples
#' data %>% mutate(date_diff = sql(calc_days_between_dates(date_1, date2)))
#'

utils::assignInNamespace(
	x = "calc_days_between_dates",
	value = calc_days_between_dates <-
		function(
			date_col_1,
			date_col_2,
			db = get_argos_default()$config("db_src")
		) {
			if (class(db)[1] == "Microsoft SQL Server") {
				sql_code <-
					paste0("DATEDIFF(day, ", date_col_1, ", ", date_col_2, ")")
			} else if (class(db)[1] == "Snowflake") {
				sql_code <-
					paste0(
						"DATEDIFF(day, ",
						'"',
						date_col_1,
						'"',
						", ",
						'"',
						date_col_2,
						'"',
						")"
					)
			} else if (class(db)[1] == "Oracle") {
				sql_code <- paste0('("', date_col_2, '" - "', date_col_1, '")')
			} else if (class(db)[1] == "src_BigQueryConnection") {
				sql_code <- paste0("DATE_DIFF(", date_col_2, ", ", date_col_1, ", DAY)")
			} else if (class(db) %in% 'SQLiteConnection') {
				sql_code <-
					paste0("julianday(", date_col_2, ") - julianday(", date_col_1, ")")
			} else if (class(db) %in% 'PrestoConnection') {
				sql_code <-
					paste0("date_diff(day, ", date_col_1, ", ", date_col_2, ")")
			} else {
				sql_code <-
					paste0(date_col_2, " - ", date_col_1)
			}
			return(sql_code)
		},
	ns = "squba.gen"
)
