#' Initialize an Argos Session
#'
#' This function creates and initializes a new Argos session with a given name and configures
#' various settings including database connection parameters, directory paths, and execution modes.
#' It dynamically adjusts configuration based on the class of the database connection and optionally
#' creates required directories.
#'
#' @param session_name A character string representing the session name.
#' @param db_conn A database connection object. If \code{is_json} is \code{TRUE}, the connection is
#'   interpreted as a JSON configuration.
#' @param is_json Logical. If \code{TRUE}, treats \code{db_conn} as JSON configuration. Default is \code{FALSE}.
#' @param base_directory A character string specifying the base directory for session files.
#' @param specs_subdirectory A character string for the specifications subdirectory. Default is \code{"code_sets"}.
#' @param results_subdirectory A character string for the results subdirectory. Default is \code{"results"}.
#' @param default_file_output Logical. If \code{TRUE}, directs output to a file rather than standard output.
#'   Default is \code{FALSE}.
#' @param cdm_schema A character string specifying the common data model schema.
#' @param results_schema A character string specifying the results schema.
#' @param vocabulary_schema A character string specifying the vocabulary schema.
#' @param results_tag An optional character string tag for naming results. Default is \code{NULL}.
#' @param cache_enabled Logical. Enables caching if \code{TRUE}. Default is \code{FALSE}.
#' @param retain_intermediates Logical. If \code{TRUE}, intermediate files are retained. Default is \code{FALSE}.
#' @param db_trace Logical. If \code{TRUE}, enables database tracing. Default is \code{TRUE}.
#' @param prep_dir Logical. If \code{TRUE}, creates the directories "code", "specs", and "results" if they do not exist.
#'   Default is \code{FALSE}.
#' @param mode A character string specifying the execution mode (e.g., \code{"development"} or \code{"production"}).
#'   Default is \code{"development"}.
#'
#' @details The function initializes an Argos session by:
#'   \itemize{
#'     \item Creating a new session using the provided \code{session_name}.
#'     \item Configuring the database source and other session parameters using \code{get_argos_default()}.
#'     \item Adjusting configuration based on the database connection type (e.g., "Snowflake", "Microsoft SQL Server", or "PqConnection").
#'     \item Setting up file directories for specifications and results.
#'     \item Optionally preparing directories if \code{prep_dir} is \code{TRUE}.
#'     \item Displaying session connection details via the \code{cli} package.
#'   }
#'
#' @return This function is used for its side effects (establishing a session and configuring the environment)
#'   and does not return a value.
#'
#' @examples
#' \dontrun{
#' initialize_session(
#'   session_name = "my_session",
#'   db_conn = my_db_connection,
#'   is_json = FALSE,
#'   base_directory = "/path/to/base",
#'   results_schema = "my_results_schema"
#' )
#' }
#'
#' @export
initialize_session <- function(session_name,
                               db_conn,
                               is_json = FALSE,
                               base_directory = "./query/",
                               specs_subdirectory = "code_sets",
                               results_subdirectory = "results",
                               default_file_output = FALSE,
                               cdm_schema = NA,
                               results_schema = NA,
                               vocabulary_schema = NA,
                               results_tag = NULL,
                               cache_enabled = FALSE,
                               retain_intermediates = FALSE,
                               db_trace = TRUE,
                               prep_dir = FALSE) {
  # call libraries
  packages <- c(
    "argos",
    "tidyverse",
    "tidyr",
    "purrr",
    "stringr",
    "srcr",
    "lubridate",
    "DBI",
    "RPostgres",
    "odbc",
    "cli",
    "pak",
    'duckdb',
    'quarto',
    'bigrquery'
  )

  for (pak in packages) {
    suppressWarnings(suppressPackageStartupMessages(require(pak, character.only = TRUE)))
  }

  # Establish session
  argos_session <- argos::argos$new(session_name)

  set_argos_default(argos_session)

  # Set db_src
    if (!is_json) {
    get_argos_default()$config("db_src", con)
  } else {
    if (jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_name == "src_bigquery") {
      # Read configuration data once to avoid repeated parsing
      config_data <- jsonlite::fromJSON(srcr::find_config_files(db_conn))
        # No valid path provided, attempt Application Default Credentials (ADC)
        # cli::cli_alert_info("No 'path_to_service_token' found or it is empty in dbconfig. Attempting BigQuery authentication using Application Default Credentials (ADC).")
        tryCatch({
          # Use default ADC lookup, specifying required scopes
          required_scopes <- c("https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform")
          bigrquery::bq_auth(scopes = required_scopes)
          cli::cli_alert_success("Successfully authenticated using Application Default Credentials.")
        }, error = function(e) {
          # Error when attempting ADC
          cli::cli_abort(c(
            "Failed to authenticate using Application Default Credentials (ADC).",
            "x" = "Ensure ADC is configured correctly for the environment (e.g., 'gcloud auth application-default login' on host and mounted to container's ADC path, or GCE metadata service).",
            "i" = paste("Original Error:", conditionMessage(e))
          ))
        })
      }
    }
    get_argos_default()$config("db_src", srcr_new(db_conn))
  }

  # Set misc configs
  if (class(get_argos_default()$config("db_src"))[1] %in% c("Snowflake")) {
    get_argos_default()$config("cdm_schema",
                               jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_arg$SCHEMA)
  } else if (class(get_argos_default()$config("db_src"))[1] %in% c("Microsoft SQL Server", "PqConnection")) {
    get_argos_default()$config(
      "cdm_schema",
      jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$db_schema
    )
  } else if (class(get_argos_default()$config("db_src"))[1] %in% c("Spark SQL")) {
    get_argos_default()$config("cdm_schema",
                               jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_args$Schema)
  } else {
    get_argos_default()$config("cdm_schema", NA)
  }

  if (class(get_argos_default()$config("db_src"))[1] == "Snowflake") {
    # Try 1 to look for defined schema in the config file
    get_argos_default()$config(
      "temp_table_schema",
      jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$temp_schema
    )

    if (get_argos_default()$config("temp_table_schema") == "") {
      get_argos_default()$config("temp_table_schema", config("cdm_schema"))
    }
  }

  get_argos_default()$config("qry_site",
                             jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$SiteName)

  if (jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_name == "src_bigquery") {
    # Store the codeset dataset name
    codeset_dataset <- jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$codeset_dataset
    
    # Check if a codeset_project is specified
    codeset_project <- jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$codeset_project
    
    # If codeset_project is specified, create a fully qualified dataset name with project
    if (!is.null(codeset_project) && nzchar(codeset_project)) {
      cli::cli_alert_info("Using codeset_project '{codeset_project}' for codesets")
      # Store the project-qualified dataset name for use in queries
      get_argos_default()$config("results_schema", paste0(codeset_project, ".", codeset_dataset))
      # Also store the unqualified name (needed for some operations)
      get_argos_default()$config("codeset_dataset_name", codeset_dataset)
      # Store the project separately
      get_argos_default()$config("codeset_project", codeset_project)
    } else {
      # No separate project specified, use the dataset name as is
      get_argos_default()$config("results_schema", codeset_dataset)
      get_argos_default()$config("codeset_dataset_name", codeset_dataset)
      get_argos_default()$config("codeset_project", NULL)
    }
  } else {
    get_argos_default()$config("results_schema", results_schema)
  }

  get_argos_default()$config("vocabulary_schema", vocabulary_schema)
  get_argos_default()$config("cache_enabled", cache_enabled)
  get_argos_default()$config("retain_intermediates", retain_intermediates)
  get_argos_default()$config("db_trace", db_trace)
  get_argos_default()$config("can_explain", !is.na(tryCatch(
    db_explain(config("db_src"), "select 1 = 1"),
    error = function(e) {
      NA
    }
  )))
  get_argos_default()$config("results_target", ifelse(default_file_output, "file", TRUE))

  if (is.null(results_tag)) {
    get_argos_default()$config("results_name_tag", "")
  } else {
    get_argos_default()$config("results_name_tag", results_tag)
  }

  # Set working directory
  get_argos_default()$config("base_dir", base_directory)

  # Set specs & results directories
  ## Drop path to base directory if present
  specs_drop_wd <- str_remove(specs_subdirectory, base_directory)
  results_drop_wd <-
    str_remove(results_subdirectory, base_directory)
  get_argos_default()$config("subdirs",
                             list(spec_dir = specs_drop_wd, result_dir = results_drop_wd))

  get_argos_default()$config("cdm_case",
                             jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$cdm_case)

  get_argos_default()$config("can_index",
                             jsonlite::fromJSON(srcr::find_config_files(db_conn))$src_site$can_index)

  if (prep_dir) {
    dir.create("code")
    dir.create("specs")
    dir.create("results")
  }
}

#' Load Query Files
#'
#' This function loads R script files containing query code from a specified directory.
#' When the execution mode is set to "production", the function uses the "code" directory
#' in the current working directory, regardless of the provided path. All R scripts
#' (files with the \code{.R} extension) in the chosen directory and its subdirectories are sourced.
#'
#' @param package A character string specifying the directory path that contains query scripts.
#'   In production mode, this parameter is ignored and the \code{"code"} directory in the current
#'   working directory is used instead.
#'
#' @details The function checks the execution mode by evaluating
#'   \code{get_argos_default()$config("execution_mode")}. If the mode is "production",
#'   it overrides the \code{package} parameter to point to \code{file.path(getwd(), "code")}.
#'   It then recursively lists all files ending in \code{.R} in the specified directory and
#'   sources each file.
#'
#' @return This function is used for its side effects (loading query scripts) and does not return a value.
#'
#' @examples
#' \dontrun{
#' # In development mode, specify the directory containing query scripts.
#' load_query("path/to/query_scripts")
#' }
#'
#' @export
load_query <- function(package) {
  files <-
    list.files(
      package,
      full.names = TRUE,
      recursive = TRUE,
      pattern = ".R$",
    )

  for (file in files) {
    if (grepl("execute_req.R|renv", file)) {
      next
    }
    source(file)
  }
}

#' `Inherited from PEDSnet/argos (https://github.com/baileych/srcr/blob/master/R/codesets.R)`
#' Create a db table with a codeset from a CSV file
#'
#' Reads the named codeset from a CSV file, and creates a like-named
#' intermediate table in the database with the contents.
#'
#' You will typically want to construct an index on the principal code column,
#' and if the codeset is very large, columns you will use to subset it
#' during use.
#'
#' Once a codeset is loaded, it is cached by name, and future calls to
#' load_codeset() with the same name will return the cached table.
#'
#' @inheritParams read_codeset
#' @param table_name An optional name for the table, if you want it to
#'   differ from `name`.
#' @param indexes A list of columns on which indexes should be created.
#' @param db A connection to the database into which to load the codeset.
#'
#' @return A tbl pointing to the table in the database
#' @export
#' @examples
#' \dontrun{
#' meds <- load_codeset('proj_meds')
#' cdm_tbl('drug_exposure') %>%
#'   semi_join(meds, by = c('drug_concept_id' = 'concept_id'))
#'
#' # Use customized structure that supports subset analyses
#' procs <- load_codeset('/proj/shared/codesets/test_procedures.csv',
#'                       col_types = 'icccc'),
#'                       table_name = 'test_proc_classes',
#'                       indexes = list('concept_id', 'procedure_class')m
#'                       full_path = TRUE)
#' cohort_procedures %>%
#'   inner_join(procs %>% filter(procedure_class %in% c('std', 'exp')),
#'              by = c('procedure_concept_id' = 'concept_id')) %>%
#'   group_by(procedure_class) %>% count()
#' }
#' @md
load_codeset <- function (name,
                          col_types = "iccc",
                          table_name = name,
                          indexes = list("concept_code"),
                          full_path = FALSE,
                          db = get_argos_default()$config("db_src"))
{
  if (get_argos_default()$config("cache_enabled")) {
    if (is.null(get_argos_default()$config("_codesets")))
      get_argos_default()$config("_codesets", list())
    cache <- get_argos_default()$config("_codesets")
    if (!is.null(cache[[name]]))
      return(cache[[name]])
  }

  # Read the codeset data first
  codeset_data <- get_argos_default()$read_codeset(name, col_types = col_types, full_path = full_path)
  
  # Check if we're using BigQuery
  if (class(get_argos_default()$config('db_src'))[1] %in% c("src_BigQueryConnection", "src_dbi", "src_sql")) {
    # Check if we have a separate codeset project specified
    codeset_project <- get_argos_default()$config("codeset_project")
    codeset_dataset <- get_argos_default()$config("codeset_dataset_name")
    
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
    } else {
      # Original behavior (same project) - using copy_to with DBI::Id
      codes <- copy_to(
        dest = db,
        df = codeset_data,
        name = DBI::Id(get_argos_default()$config('results_schema'), name),
        overwrite = TRUE,
        temporary = FALSE
      )
    }
  } else {
    # For non-BigQuery connections, use the original approach
    codes <- get_argos_default()$copy_to_new(
      db,
      codeset_data,
      name = table_name,
      overwrite = TRUE,
      indexes = indexes
    )
  }

  if (get_argos_default()$config("cache_enabled")) {
    cache[[name]] <- codes
    get_argos_default()$config("_codesets", cache)
  }
  codes
}

#' `Inherited from srcr (https://github.com/baileych/srcr/blob/master/R/create_src.R)`
#' Modification to allow for interactive password entry during PAQS sessions
#' Connect to database using config file
#'
#' Set up a or DBI or legacy dplyr database connection using information from a
#' JSON configuration file, and return the connection.
#'
#' The configuration file must provide all of the information necessary to set
#' up the DBI connection or dplyr src.  Given the variety of ways a data source
#' can be specified, the JSON must be a hash containing at least two elements:
#'
#' * The `src_name` key points to a string containing name of a DBI driver
#'   method (e.g. `SQLite`), as one might pass to [DBI::dbDriver()], or an old-style
#'   dplyr function that sets up the data source (e.g.  [dplyr::src_postgres()].
#'   If the value associated with `src_name` begins with 'src_', it is taken as the
#'   latter, otherwise it is taken as the former.  In this case, an attempt will
#'   be made to load the appropriate DBI-compliant database library (e.g. RSQLite
#'   for the above example) if it hasn't already been loaded.
#' * The `src_args` key points to a nested hash, whose keys are the arguments
#'   to that function, and whose values are the argument values.
#'
#' To locate the necessary configuration file, you can use all of the arguments
#' taken by [find_config_files()], but remember that the contents of the file
#' must be JSON, regardless of the file's name.  Alternatively, if `paths` is
#' present, only the specified paths are checked. The first file that exists, is
#' readable, and evaluates as legal JSON is used as the source of configuration
#' data.
#'
#' If your deployment strategy does not make use of configuration files (e.g. you
#' access configuration data via a web service or similar API), you may also
#' pass a list containing the configuration data directly via the `config`
#' parameter.  In this case, no configuration files are used.
#'
#' Because some uses may require additional actions, such as setting up
#' environment variables, external authentication, or initialization work within
#' the database session, you may include code to be executed in your
#' configuration file.  The `pre_connect_fun` element, if present, should be an
#' array of text that will be joined linewise and evaluated as R source code. It
#' must define an anonymouis function which will be called with one argument,
#' the content of the config file.  If this function returns a DBI connection,
#' the srcr will skip the default process for creating a connection and use this
#' instead. Any other non-NA return value replaces the configuration data
#' originally read from the file during further steps.  Once the connection is
#' established, the `post_connect_sql` and `post_connect_fun` elements of the
#' configuration data can be used to perform  additional processing to set
#' session characteristics, roles, etc.  However,  because this entails the
#' configuration file providing code that you won't see  prior to runtime, you
#' need to opt in to these features.  You can make this choice globally by
#' setting the `srcr.allow_config_code` option via  [base::options()], or you
#' can enable it on a per-call basis with the `allow_config_code` parameter.
#'
#' @inheritParams find_config_files
#' @param paths A vector of full path names for the configuration file.  If
#'   present, only these paths are checked; [find_config_files()] is not called.
#' @param config A list containing the configuration data, to be used instead of
#'   reading a configuration file, should you wish to skip that step.
#' @param allow_config_code A vector specifying what session setup you will
#'   permit via code contained in the config.  If any element of the vector
#'   is `sql`, then the post_connect_sql section of the configuration file is
#'   executed aftern the connection is established. If any element is `fun`,
#'   then the pre- and post-connection functions will be executed (see above).
#' @param allow_post_connect `r lifecycle::badge('deprecated')`
#'   This has been superseded by the more  generally functional
#'   `allow_config_code` parameter.  It currently generates a warning when used,
#'   and will be removed in a future version.
#'
#' @return A database connection.  The specific class of the object is determined
#'   by the `src_name` in the configuration data.
#'
#' @examples
#' \dontrun{
#' # Search all the (filename-based) defaults
#' srcr()
#'
#' # "The usual"
#' srcr("myproj_prod")
#'
#' # Look around
#' srcr(
#'   dirs = c(Sys.getenv("PROJ_CONF_DIR"), "var/lib", getwd()),
#'   basenames = c("myproj", Sys.getenv("PROJ_NAME"))
#' )
#'
#' # No defaults
#' srcr(paths = c("/path/to/known/config.json"))
#' srcr(
#'   config =
#'     list(
#'       src_name = "Postgres",
#'       src_args = list(host = "my.host", dbname = "my_db", user = "me"),
#'       post_connect_sql = "set role project_role;"
#'     ),
#'   allow_config_code = "sql"
#' )
#' }
#' @export
#' @md
srcr_new <-
  function(basenames = NA,
           dirs = NA,
           suffices = NA,
           paths = NA,
           config = NA,
           allow_post_connect =
             getOption("srcr.allow_post_connect", c())) {
    if (is.na(config)) {
      if (is.na(paths)) {
        args <- mget(c("dirs", "basenames", "suffices"))
        args <- args[!is.na(args)]
        paths <- do.call(srcr::find_config_files, args)
        if (length(paths) < 1) {
          stop(
            "No config files found for ",
            paste(
              vapply(names(args),
                function(x) {
                  paste(
                    x, "=",
                    paste(args[[x]],
                      collapse = ", "
                    )
                  )
                },
                FUN.VALUE = character(1)
              ),
              collapse = "; "
            )
          )
        }
      }

      .read_json_config <- function(paths = c()) {
        if (length(paths) < 1) {
          stop("No config paths provided")
        }
        for (p in paths) {
          config <- tryCatch(
            jsonlite::fromJSON(p),
            error = function(e) {
              NA
            }
          )
          if (!is.na(config[1])) {
            return(config)
          }
        }
        stop(
          "No valid config files found in ",
          paste(paths, collapse = ", ")
        )
      }

      config <- .read_json_config(paths)

      if (any(config$src_args == "DO_NOT_EDIT_IF_YOU_WISH_TO_ENTER_IT_ON_DOCKER_RUN")) {
        if (config$src_name == "Postgres") {
          config$src_args$password <- getPass::getPass(msg = paste0("Enter Password for ", config$src_args$user, " : "))
        }
        if (config$src_name == "odbc") {
          if (config$src_args$Driver == "SnowflakeDSIIDriver") {
            config$src_args$PWD <- getPass::getPass(msg = paste0("Enter Password for ", config$src_args$UID, " : "))
          }

          if (config$src_args$Driver == "ODBC Driver 18 for SQL Server" && config$src_args$Trusted_Connection == "yes") {
            config$src_args$UID <- NULL
            config$src_args$PWD <- NULL
          }

          if (config$src_args$Driver == "ODBC Driver 18 for SQL Server" && config$src_args$Trusted_Connection == "N/A") {
            config$src_args$Trusted_Connection <- NULL
            config$src_args$PWD <- getPass::getPass(msg = paste0("Enter Password for ", config$src_args$UID, " : "))
          }
        }
      } else if ((config$src_args$password != "DO_NOT_EDIT_IF_YOU_WISH_TO_ENTER_IT_ON_DOCKER_RUN" ||
        config$src_args$PWD != "DO_NOT_EDIT_IF_YOU_WISH_TO_ENTER_IT_ON_DOCKER_RUN") &&
        any(names(config$src_args) == "Trusted_Connection")) {
        config$src_args$Trusted_Connection <- NULL
      }
    }

    if (!grepl("^src_", config$src_name)) {
      drv <- tryCatch(DBI::dbDriver(config$src_name), error = function(e) NULL)
      if (is.null(drv)) {
        lib <- tryCatch(
          library(config$src_name,
            character.only = TRUE
          ),
          error = function(e) NULL
        )
        if (is.null(lib)) {
          library(paste0("R", config$src_name), character.only = TRUE)
        }
        drv <- DBI::dbDriver(config$src_name)
      }
      config$src_name <- function(...) DBI::dbConnect(drv, ...)
    }

    db <- do.call(config$src_name, config$src_args)

    if (any(allow_post_connect == "sql") &&
      exists("post_connect_sql", where = config)) {
      con <- if (inherits(db, "src_dbi")) db$con else db
      lapply(config$post_connect_sql, function(x) DBI::dbExecute(con, x))
    }
    if (any(allow_post_connect == "fun") &&
      exists("post_connect_fun", where = config)) {
      pc <- eval(parse(text = paste(config$post_connect_fun,
        sep = "\n", collapse = "\n"
      )))
      db <- do.call(pc, list(db))
    }
    db
  }



#' Create PAQS Package
#'
#' This function executes a shell script to build a package. It uses the \code{system2} function to
#' call the \code{bash} shell script located at \code{"shell/build_package.sh"}.
#'
#'
#' @details This function does not require any parameters. It is intended as a convenient wrapper to run the
#'   package build process via a shell script. Ensure that the shell script has executable permissions and that
#'   \code{bash} is available in your system.
#'
#' @examples
#' \dontrun{
#' # Execute the package build script
#' create_paqs_package()
#' }
#' @export
create_paqs_package <- function() {
  system2("tools/build_package.sh")
}


init_message <- function(query_title) {
  # Print the formatted banner with newlines
  if (.Platform$OS.type	== "windows") {
    cat(readLines("./tools/banner", warn = FALSE), sep = "\n")
  } else {
    cat(
      "\n",
      "\033[38;5;22m",
      readLines("./tools/banner", warn = FALSE),
      "\033[0m",
      "\n",
      sep = "\n"
    )
  }
  Sys.sleep(1)

  cli::cli_div(theme = list(
    span.danger = list(color = "red"),
    span.success = list(color = "green")
  ))


  SiteName <- jsonlite::fromJSON(srcr::find_config_files("dbconfig"))$src_site$SiteName
  src_name <- jsonlite::fromJSON(srcr::find_config_files("dbconfig"))$src_name
  Driver <- jsonlite::fromJSON(srcr::find_config_files("dbconfig"))$src_args$Driver

  mklist <- function() {
    cli::cli_ul()
    cli::cli_li("Execution Details:")
    cli::cli_ul()
    cli::cli_li(paste0("Site :", SiteName))
    cli::cli_li(paste0("Database :", src_name))
    if (!is.null('Driver'))
      cli::cli_li(paste0("Driver :", Driver))
    cli::cli_end()
  }
  cli:::cli_h1("PCORnet® Advanced Querying System (PAQS) v0.1")
  cli:::cli_h3(query_title)
  cli::cli_h1("")
  cli::cli_text('\n\n')
  mklist()
  cli::cli_h1("")
  cli::cli_text('\n\n')
  Sys.sleep(2)
}
