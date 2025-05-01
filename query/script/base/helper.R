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
get_table_one <- function(cohort,
													cat_vars = NULL,
													cont_var = NULL) {
	result <- list()

	if (is.null(cat_vars) &
			is.null(cont_var)) {
		stop("Table one needs alteast one continuous or a categorical variable specified")
	}

	if (!is.null(cat_vars)) {
		for (i in cat_vars) {
			result[[i]] <- cohort %>% count(!!sym(i)) %>%
				mutate(
					variable = paste0(i, '_', !!sym(i)),
					block = i,
					var_type = "cat"
				) %>%
				rename(Characteristics = !!sym(i),
							 value = n)
		}
	}

	if (!is.null(cont_var)) {
		for (i in cont_var) {
			result[[i]] <- cohort %>%
				summarise(
					mean = mean(!!sym(i), na.rm = T),
					sd = sd(!!sym(i), na.rm = T),
					median = median(!!sym(i)),
					quantile_25 = quantile(!!sym(i), 0.25),
					quantile_75 = quantile(!!sym(i), 0.75),
					block = i,
					var_type = "cont"
				) %>%
				tidyr::pivot_longer(
					cols = c("mean", "median", "sd", "quantile_25", "quantile_75"),
					values_to = "value"
				) %>%
				mutate(variable = paste0(block, name)) %>%
				rename(Characteristics = name)
		}
	}
result[["total"]] <- cohort %>% count() %>%
	mutate(block = "Total",
				 variable = "Total",
				 Characteristics = "Total",
				 var_type = "Total") %>%
	rename(value = n)

	table_1 <- reduce(result, union_all) %>%
		select(block,variable,Characteristics,var_type,value)
	return(table_1)
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

	if(.Platform$OS.type	== "windows") {

	sys.echo <-
		paste0("system('echo ", text, "')")

	source(textConnection(sys.echo))

	} else {
	script <- in_memory_bash_cli_library()

	bash_styles <- c(
		info    = "cli_info",
		success = "cli_success",
		warning = "cli_warning",
		error   = "cli_error",
		heading = "cli_heading"
	)

	if (!style %in% names(bash_styles)) {
		stop("Unknown style '", style, "'. Valid styles are: ",
				 paste(names(bash_styles), collapse = ", "), ".")
	}

	bash_function <- bash_styles[[style]]

	cmd <- sprintf(
		"bash << 'EOF'\n%s\n%s \"%s\"\nEOF",
		script, bash_function, text
	)

	system(cmd)
	
}
	r_styles <- list(
		info    = cli::cli_alert_info,
		success = cli::cli_alert_success,
		warning = cli::cli_alert_warning,
		error   = cli::cli_alert_danger,
		heading = function(txt) cli::cli_h1(txt)
	)

	r_styles[[style]](text)
	}

show_progress <- function(step, message) {
	progress_pct <- round(step / .GlobalEnv$total_steps * 100, 0)

	if(.Platform$OS.type	== "windows") {
		text <- paste0("PROGRESS: ", progress_pct, "%\n")

		sys.echo <-
		paste0("system('echo ", text, "')")

	source(textConnection(sys.echo))
		
	} else {

	echo_text(paste0("PROGRESS: ", progress_pct, "%\n"))
		
	}

	cli::cli_h1(paste0("Step ", step, "/", total_steps, ": ", message))
	
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
	function(date_col_1, date_col_2, db = get_argos_default()$config("db_src")) {

		if (class(db)[1] == "Microsoft SQL Server") {
			sql_code <-
				paste0("DATEDIFF(day, ", date_col_1, ", ", date_col_2, ")")
		} else if (class(db)[1] == "Snowflake") {
			sql_code <-
				paste0("DATEDIFF(day, ",'"',date_col_1,'"',", ",'"',date_col_2,'"',")")
		} else if (class(db)[1] == "Oracle") {
			sql_code <- paste0('("', date_col_2, '" - "', date_col_1, '")')
		} else if (class(db)[1] == "src_BigQueryConnection") {
			sql_code <- paste0("DATE_DIFF(",date_col_1,", ",date_col_2,", DAY)")
		} else if(class(db) %in% 'SQLiteConnection'){
			sql_code <-
				paste0("julianday(", date_col_2, ") - julianday(", date_col_1, ")")
		}else if(class(db) %in% 'PrestoConnection'){
			sql_code <-
				paste0("date_diff(day, ", date_col_1, ", ", date_col_2, ")")
		} else {
			sql_code <-
				paste0(date_col_2, " - ", date_col_1)
		}
		return(sql_code)
	},
	ns = "squba.gen")