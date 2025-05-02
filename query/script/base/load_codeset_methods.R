patch_argos2 <- function() {
  argos$public_methods$load_codeset <- function(name,
                                                col_types = "iccc",
                                                table_name = name,
                                                indexes = list("concept_code"),
                                                full_path = FALSE,
                                                db = self$config("db_src")) {
    conn_class <- class(db)[1]
    method_name <- paste0("load_codeset.", conn_class)


    if (method_name %in% names(self)) {
      return(self[[method_name]](name, col_types, table_name, indexes, full_path, db))
    } else {
      return(self$`load_codeset.default`(name, col_types, table_name, indexes, full_path, db))
    }
  }

argos$public_methods$`load_codeset.default` <-  function(name,
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

    if (self$config("cache_enabled")) {
      cache[[name]] <- codes
      self$config("_codesets", cache)
    }
    codes
}


argos$public_methods$`load_codeset.Oracle` <- function(name,
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

    if (self$config("cache_enabled")) {
      cache[[name]] <- codes
      self$config("_codesets", cache)
    }
    codes
  }

argos$public_methods$`load_codeset.src_BigQueryConnection` <- function(name,
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
      self$config(
        "temp_table_drop_me",
        append(self$config("temp_table_drop_me"), name)
      )

     # Check if we have a separate codeset project specified
     codeset_project <- self$config("codeset_project")
     codeset_dataset <- self$config("codeset_dataset_name")

     if (!is.null(codeset_project) && nzchar(codeset_project)) {
       # Use bigrquery's native functions for more direct control
       cli::cli_alert_info("Creating codeset table '{name}' in project '{codeset_project}', dataset '{codeset_dataset}'")

       # Create a BigQuery table reference with explicit project and dataset
       bq_conn <- bigrquery::bq_project_query(codeset_project, "SELECT 1")
       table_id <- bigrquery::bq_table(project = codeset_project, dataset = codeset_dataset, table = name)

       # Delete the table if it exists (for overwrite)
       if (bigrquery::bq_table_exists(table_id)) {
         bigrquery::bq_table_delete(table_id)
       }

       # Upload data to BigQuery table
       bigrquery::bq_table_upload(table_id, codeset_data, write_disposition = "WRITE_TRUNCATE")

       # Create a dplyr reference to the table with explicit project.dataset.table format
       qualified_table_name <- paste0("`", codeset_project, ".", codeset_dataset, ".", name, "`")
       # Fix for BigQuery syntax - use standard BigQuery syntax without parentheses
       sql_query <- paste0("SELECT * FROM ", qualified_table_name, " as q")
       codes <- dplyr::tbl(db, dplyr::sql(sql_query))

    if (self$config("cache_enabled")) {
      cache[[name]] <- codes
      self$config("_codesets", cache)
    }
    codes
  }
}
}