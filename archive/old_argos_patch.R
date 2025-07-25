#' Modifications to argos package for extending functionality to other databases
patch_argos <- function() {
	argos$public_methods$load_codeset <- function(name,
																								col_types = "iccc",
																								table_name = name,
																								indexes = list("concept_code"),
																								full_path = FALSE,
																								db = self$config("db_src")) {
		if (self$config("cache_enabled")) {
			if (is.null(self$config("_codesets"))) {
				self$config("_codesets", list())
			}
			cache <- self$config("_codesets")
			if (!is.null(cache[[name]])) {
				return(cache[[name]])
			}
		}

		if (self$config_exists("can_index")) {
			if (!self$config("can_index")) {
				indexes <- NULL
			}
		}

		if (
			!class(self$config("db_src"))[1] %in%
			c("Oracle", "src_BigQueryConnection")
		) {
			codes <- self$copy_to_new(
				db,
				self$read_codeset(
					name,
					col_types = col_types,
					full_path = full_path
				),
				name = table_name,
				overwrite = TRUE,
				indexes = indexes
			)
		} else {
			self$config(
				"temp_table_drop_me",
				append(self$config("temp_table_drop_me"), name)
			)

			codes <- copy_to(
				dest = db,
				df = self$read_codeset(name, col_types = col_types, full_path = full_path),
				name = dbplyr::in_schema(self$config("temp_table_schema"), name),
				overwrite = TRUE,
				temporary = FALSE,
				indexes = indexes
			)
		}

		if (self$config("cache_enabled")) {
			cache[[name]] <- codes
			self$config("_codesets", cache)
		}
		codes
	}


	argos$public_methods$compute_new <- function(tblx,
																							 name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
																							 temporary = !self$config("retain_intermediates"),
																							 ...) {
		if (!inherits(name, c("ident_q", "dbplyr_schema")) && length(name) == 1) {
			name <- gsub("\\s+", "_", name, perl = TRUE)
			name <- self$intermed_name(name, temporary)
		}
		con <- self$dbi_con(tblx)
		#' Apperantly MS SQL pads temperory table name with `#` so we modify the name
		#' to make sure that following db_exists_new and db_remove_new logic doesn't fail.

		# Broken in dbplyr, so do it ourselves
		if (class(self$config("db_src")) == "Microsoft SQL Server" && temporary) {
			if (self$db_exists_table(con, paste0("#", name))) {
				self$db_remove_table(con, paste0("#", name))
			}
		} else {
			if (self$db_exists_table(con, name)) {
				self$db_remove_table(con, name)
			}
		}
		if (self$config("db_trace")) {
			show_query(tblx)
			if (self$config("can_explain")) explain(tblx)
			message(
				" -> ",
				base::ifelse(packageVersion("dbplyr") < "2.0.0", dbplyr::as.sql(name), dbplyr::as.sql(name, con))
			)
			start <- Sys.time()
			message(start)
		}

		ellipsis_args <- list(...)
		if (self$config_exists("can_index")) {
			if (!self$config("can_index")) {
				ellipsis_args$indexes <- c()
			}
		}

		if (
			!class(self$config("db_src"))[1] %in%
			c("Oracle", "src_BigQueryConnection")
		) {
			rslt <-
				do.call(dplyr::compute, c(list(tblx, name = name, temporary = temporary), ellipsis_args))
		} else {
			self$config("temp_table_drop_me", append(self$config("temp_table_drop_me"), name))

			rslt <-
				do.call(
					dplyr::copy_to,
					c(
						list(con, tblx, name = dbplyr::in_schema(self$config("temp_table_schema"), name), overwrite = TRUE, temporary = FALSE),
						ellipsis_args
					)
				)
		}

		if (self$config("db_trace")) {
			end <- Sys.time()
			message(end, " ==> ", format(end - start))
		}
		rslt
	}


	argos$public_methods$copy_to_new <- function(dest = self$config("db_src"), df, name = deparse(substitute(df)),
																							 overwrite = TRUE, temporary = !self$config("retain_intermediates"),
																							 ...) {
		name <- self$intermed_name(name, temporary = temporary)
		if (self$config("db_trace")) {
			message(" -> copy_to")
			start <- Sys.time()
			message(start)
			message("Data: ", deparse(substitute(df)))
			message("Table name: ", base::ifelse(packageVersion("dbplyr") <
																					 	"2.0.0", dbplyr::as.sql(name), dbplyr::as.sql(
																					 		name,
																					 		dbi_con(dest)
																					 	)), " (temp: ", temporary, ")")
			message("Data elements: ", paste(tbl_vars(df), collapse = ","))
			message("Rows: ", NROW(df))
		}
		if (class(self$config("db_src")) == "Microsoft SQL Server" && temporary) {
			if (overwrite && self$db_exists_table(dest, paste0("#", name))) {
				self$db_remove_table(dest, paste0("#", name))
			}
		} else {
			if (overwrite && self$db_exists_table(dest, name)) {
				self$db_remove_table(dest, name)
			}
		}


		ellipsis_args <- list(...)
		if (self$config_exists("can_index")) {
			if (!self$config("can_index")) {
				ellipsis_args$indexes <- c()
			}
		}
		if (
			!class(self$config("db_src"))[1] %in%
			c("Oracle", "src_BigQueryConnection")
		) {
			rslt <- do.call(dplyr::copy_to, (c(
				list(dest = dest, df = df, name = name, overwrite = overwrite, temporary = temporary), ellipsis_args
			)))
		} else {
			self$config(
				"temp_table_drop_me",
				append(self$config("temp_table_drop_me"), name)
			)

			rslt <-
				do.call(
					dplyr::copy_to,
					c(
						list(dest, df,
								 name = dbplyr::in_schema(self$config("temp_table_schema"), name), overwrite = TRUE, temporary = FALSE
						),
						ellipsis_args
					)
				)
		}
		if (self$config("db_trace")) {
			end <- Sys.time()
			message(end, " ==> ", format(end - start))
		}
		rslt
	}

	argos$private_methods$.ora_env_setup <-
		function(db) {
			DBI::dbExecute(db, "alter session set nls_date_format = 'YYYY-MM-DD'")
			DBI::dbExecute(
				db,
				"alter session set nls_timestamp_tz_format = 'YYYY-MM-DD HH24:MI:SS TZHTZM'"
			)
		}


	argos$private_methods$.ora_env_cleanup <- function(db) {
		dm <- self$config("temp_table_drop_me")
		if (length(dm) == 0) {
			return(invisible(TRUE))
		}

		vapply(
			unique(dm),
			function(t) {
				tryCatch(
					get_argos_default()$db_remove_table(db, t, temporary = TRUE),
					error = function(e) {
						message(t, " - ", e)
						0L
					}
				)
			},
			FUN.VALUE = 0L
		)
	}


	argos$public_methods$qual_name <-
		function(name, schema_tag, db = self$config("db_src")) {
			if (inherits(name, c("ident_q", "dbplyr_schema"))) {
				return(name)
			}
			name_map <- self$config("table_names")
			name <- base::ifelse(hasName(name_map, name), name_map[[name]], name)
			if (self$config_exists("cdm_case")) {
				name <- base::ifelse(self$config("cdm_case") == "upper", toupper(name), name)
			}
			if (!is.na(schema_tag)) {
				if (self$config_exists(schema_tag)) schema_tag <- self$config(schema_tag)
				if (!is.na(schema_tag)) {
					if (packageVersion("dbplyr") < "2.0.0") { # nocov start
						name <- DBI::dbQuoteIdentifier(self$dbi_con(db), name)
					} # nocov end
					name <- dbplyr::in_schema(schema_tag, name)
				}
			}
			name
		}



	#' Correct Table Column Names to Lowercase
	#'
	#' This function checks the column names of a data frame or tibble and converts them to lowercase if any are not already.
	#'
	#' @param tbl A data frame or tibble whose column names will be verified and potentially converted to lowercase.
	#'
	#' @return A data frame or tibble with all column names in lowercase. If the column names are already lowercase, the original table is returned.
	#'
	#'
	#' @export
	#' @md
	argos$public_methods$tbl_case_corrector <-
		function(tbl) {
			if (self$config_exists("cdm_case")) {
				if (self$config("cdm_case") == "upper") {
					return(tbl %>% rename_all(~ tolower(.)))
				} else {
					return(tbl)
				}
			} else {
				return(tbl)
			}
		}

	argos$public_methods$cdm_tbl <- function(name, db = self$config("db_src")) {
		self$tbl_case_corrector(self$qual_tbl(name, "cdm_schema", db))
	}
}