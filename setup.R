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
initialize_session <- function(
  session_name,
  db_conn,
  query_title = "",
  base_directory = "./query/",
  specs_subdirectory = "code_sets",
  results_subdirectory = "results",
  default_file_output = FALSE,
  cdm_schema = NA,
  table_names,
  results_schema = NA,
  vocabulary_schema = NA,
  results_tag = NULL,
  cache_enabled = FALSE,
  retain_intermediates = FALSE,
  db_trace = TRUE,
  prep_dir = FALSE
) {
  # Starting session initialization
  init_banner(query_title = query_title)

  cli::cli_alert_info("Loading dependencies...")
  source('query/script/base/dependencies.R')

  cli::cli_alert_info("Establishing argos session...")
  argos_session <- argos::argos$new(session_name)
  set_argos_default(argos_session)

  cli::cli_alert_info("Configuring table names...")
  get_argos_default()$config('table_names', table_names)

  cli::cli_alert_info("Setting up database connection...")
  get_argos_default()$config("db_src", srcr_helper(db_conn))

  db_class <- class(get_argos_default()$config("db_src"))[1]
  cli::cli_alert_success("Database type detected: {.strong {db_class}}")

  if (db_class == "Oracle") {
    cli::cli_alert_info("Configuring Oracle environment...")
    get_argos_default()$.__enclos_env__$private$.ora_env_setup(
      db = get_argos_default()$config("db_src")
    )
    cli::cli_alert_success("Oracle environment configured")
  }

  cli::cli_alert_info("Reading database configuration...")
  config_data <- jsonlite::fromJSON(Sys.getenv('config_path'))

  # Set CDM schema based on database type
  cli::cli_alert_info("Determining CDM schema for {.strong {db_class}}...")
  cdm_schema <- switch(
    db_class,
    "Snowflake" = config_data$src_arg$SCHEMA,
    "Microsoft SQL Server" = config_data$src_site$db_schema,
    "PqConnection" = config_data$src_site$db_schema,
    "Oracle" = config_data$src_site$db_schema,
    "OraConnection" = config_data$src_site$db_schema,
    "Spark SQL" = config_data$src_args$Schema,
    "src_BigQueryConnection" = config_data$src_args$dataset,
    NA
  )

  if (!is.na(cdm_schema)) {
    cli::cli_alert_success("CDM schema set: {.strong {cdm_schema}}")
  } else {
    cli::cli_alert_warning("CDM schema not configured")
  }

  get_argos_default()$config("cdm_schema", cdm_schema)

  cli::cli_alert_info("Configuring temporary schema...")
  temp_schema <- switch(
    db_class,
    "Snowflake" = config_data$src_site$temp_schema,
    "Oracle" = config_data$src_site$temp_schema,
    "OraConnection" = config_data$src_site$temp_schema,
    "Spark SQL" = config_data$src_site$temp_schema,
    "src_BigQueryConnection" = {
      codeset_dataset <- config_data$src_site$codeset_dataset
      codeset_project <- config_data$src_site$codeset_project

      if (!is.null(codeset_project) && nzchar(codeset_project)) {
        get_argos_default()$config("codeset_dataset_name", codeset_dataset)
        get_argos_default()$config("codeset_project", codeset_project)
        cli::cli_alert_success(
          "Using codeset project: {.strong {codeset_project}}.{codeset_dataset}"
        )
        paste0(codeset_project, ".", codeset_dataset)
      } else {
        get_argos_default()$config("codeset_dataset_name", codeset_dataset)
        get_argos_default()$config("codeset_project", NULL)
        cli::cli_alert_info(
          "Using codeset dataset: {.strong {codeset_dataset}}"
        )
        codeset_dataset
      }
    },
    cdm_schema
  )

  cli::cli_alert_success("Temporary schema: {.strong {temp_schema}}")

  get_argos_default()$config("all_available_schemas", temp_schema)
  get_argos_default()$config("temp_table_schema", temp_schema[1])


  if(db_class == 'Spark SQL') {
    local_test <- ifelse(is.null(config_data$src_site$local_test),FALSE,TRUE)
    get_argos_default()$config("local_test", local_test)
  }

  get_argos_default()$config('temp_table_drop_me', character(0))

  # Site configuration
  site_name <- config_data$src_site$SiteName

  cli::cli_alert_success("Site configured: {.strong {site_name}}")
  get_argos_default()$config("qry_site", site_name)

  # Schema configurations
  if (!is.na(results_schema)) {
    cli::cli_alert_info("Results schema: {.strong {results_schema}}")
  }
  get_argos_default()$config("results_schema", results_schema)

  if (!is.na(vocabulary_schema)) {
    cli::cli_alert_info("Vocabulary schema: {.strong {vocabulary_schema}}")
  }
  get_argos_default()$config("vocabulary_schema", vocabulary_schema)

  # Feature flags
  cli::cli_alert_info("Configuring session options...")
  get_argos_default()$config("cache_enabled", cache_enabled)
  get_argos_default()$config("retain_intermediates", retain_intermediates)
  get_argos_default()$config("db_trace", db_trace)

  cli::cli_bullets(c(
    "v" = "Cache: {.strong {ifelse(cache_enabled, 'enabled', 'disabled')}}",
    "v" = "Retain intermediates: {.strong {ifelse(retain_intermediates, 'yes', 'no')}}",
    "v" = "DB trace: {.strong {ifelse(db_trace, 'enabled', 'disabled')}}"
  ))

  # Test EXPLAIN capability
  cli::cli_alert_info("Testing database EXPLAIN capability...")
  can_explain <- !is.na(tryCatch(
    db_explain(config("db_src"), "select 1 = 1"),
    error = function(e) NA
  ))
  get_argos_default()$config("can_explain", can_explain)

  if (can_explain) {
    cli::cli_alert_success("EXPLAIN queries supported")
  } else {
    cli::cli_alert_warning("EXPLAIN queries not supported")
  }

  # Results configuration
  results_target <- ifelse(default_file_output, "file", TRUE)
  cli::cli_alert_info(
    "Results output: {.strong {ifelse(default_file_output, 'file', 'database')}}"
  )
  get_argos_default()$config("results_target", results_target)

  if (is.null(results_tag)) {
    get_argos_default()$config("results_name_tag", "")
  } else {
    cli::cli_alert_info("Results tag: {.strong {results_tag}}")
    get_argos_default()$config("results_name_tag", results_tag)
  }

  # Directory configuration
  cli::cli_alert_info("Configuring directories...")
  get_argos_default()$config("base_dir", base_directory)

  specs_drop_wd <- str_remove(specs_subdirectory, base_directory)
  results_drop_wd <- str_remove(results_subdirectory, base_directory)

  get_argos_default()$config(
    "subdirs",
    list(
      spec_dir = specs_drop_wd,
      result_dir = results_drop_wd
    )
  )

  cli::cli_bullets(c(
    "v" = "Base: {.path {base_directory}}",
    "v" = "Specs: {.path {specs_drop_wd}}",
    "v" = "Results: {.path {results_drop_wd}}"
  ))

  # CDM case configuration
  cdm_case <- config_data$src_site$cdm_case
  cli::cli_alert_info("CDM case: {.strong {cdm_case}}")
  get_argos_default()$config("cdm_case", cdm_case)

  # Index capability
  index_val <- config_data$src_site$can_index
  if (!is.logical(index_val)) {
    index_val <- as.logical(index_val)
  }
  get_argos_default()$config("can_index", index_val)
  cli::cli_alert_info(
    "Indexing: {.strong {ifelse(index_val, 'supported', 'not supported')}}"
  )

  rm(config_data)
  gc(verbose = FALSE)

  # Final initialization message
  cli::cli_alert_success("Session initialization complete!")
  cli::cli_rule()
}

#======
srcr_helper <- function(db_conn) {
  home_dir <- Sys.getenv('HOME')

  # Inform user we're searching default locations
  cli::cli_alert_info(
    "Searching for database configuration: {.strong {db_conn}}"
  )
  cli::cli_ul(c(
    "{.path {file.path(home_dir, paste0(db_conn))}}",
    "{.path {file.path(home_dir, '.srcr', paste0(db_conn))}}"
  ))

  # First, try to find config in default locations
  config_path <- tryCatch(
    {
      srcr::find_config_files(db_conn)
    },
    error = function(e) {
      cli::cli_alert_warning("Config search error: {conditionMessage(e)}")
      NULL
    }
  )

  # If found, return the path
  if (!is.null(config_path) && file.exists(config_path)) {
    src_name <- tryCatch({
      jsonlite::fromJSON(config_path)$src_name
    }, error = function(e) {
      cli::cli_alert_warning("Config search error: {conditionMessage(e)}")
      NULL
    })


    if (!is.null(src_name)) {
      cli::cli_alert_success("Found configuration: {.file {config_path}}")
      cli::cli_alert_info("Connection type: {.strong {src_name}}")

    if (src_name == "src_bigquery") {
      authenticate_bigquery()
    }
    Sys.setenv('config_path' = config_path)

    return(srcr::srcr(db_conn))

    } else {
      if (Sys.getenv('execution_mode') == "container") {
        cli_div(theme = list(span.emph = list(color = "red")))
        cli_abort(
          "Ensure right config file is mounted in {.code docker run --rm -it -v {.emph path/to/dbconfig}:/root/dbconfig} [Including the name {.code dbconfig} {.emph path/to/dbconfig}. Additionally, check if you have a extension such as {.code .json}, {.code .txt}"
        )
      }
    }
  }

  # If not found, handle based on session type
  cli::cli_alert_warning("Configuration not found in default locations")

  # Check if we're in an interactive session
  if (!interactive()) {
    cli::cli_abort(c(
      "Database configuration not found",
      "x" = "Config '{db_conn}' not found in default locations",
      "i" = "Expected locations:",
      " " = "{.path {file.path(home_dir, paste0(db_conn))}}",
      " " = "{.path {file.path(home_dir, '.srcr', paste0(db_conn))}}",
      "i" = "Running in non-interactive mode - cannot prompt for file"
    ))
  }

  # Interactive mode - prompt for custom path
  custom_path <- prompt_for_config_path()

  # Validate the provided path
  if (is.null(custom_path) || custom_path == "" || !file.exists(custom_path)) {
    cli::cli_abort(c(
      "Invalid file path",
      "x" = "File does not exist or path was empty",
      "i" = "Path provided: {.path {custom_path}}"
    ))
  }

  cli::cli_alert_success("Using configuration: {.file {custom_path}}")

  # Read configuration and authenticate if needed
  src_name <- jsonlite::fromJSON(custom_path)$src_name
  cli::cli_alert_info("Connection type: {.strong {src_name}}")

  if (src_name == "src_bigquery") {
    authenticate_bigquery()
  }

  Sys.setenv('config_path' = custom_path)

  return(srcr::srcr(paths = custom_path))
}

# Helper function to prompt for config path
prompt_for_config_path <- function() {
  cli::cli_h2("Manual Configuration Path Required")

  # Check if we have GUI capabilities (X11, Wayland, etc.)
  has_gui <- .Platform$GUI == "RStudio"

  if (
    has_gui && Sys.getenv('execution_mode') %in% c("nativeR", 'development')
  ) {
    cli::cli_alert_info("Opening file picker...")
    return(tryCatch(
      file.choose(),
      error = function(e) {
        cli::cli_alert_warning("File picker failed or cancelled")
        NULL
      }
    ))
  } else {
    if (Sys.getenv('execution_mode') == "container") {
      cli::cli_alert_info("Running query in a container")
      return('/root/dbconfig')
    }
  }
}

# Helper function for BigQuery authentication
authenticate_bigquery <- function() {
  cli::cli_alert_info("Authenticating with BigQuery...")
  tryCatch(
    {
      required_scopes <- c(
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform"
      )
      bigrquery::bq_auth(scopes = required_scopes)
      cli::cli_alert_success("BigQuery authentication successful (ADC)")
    },
    error = function(e) {
      cli::cli_abort(c(
        "BigQuery authentication failed",
        "x" = "Could not authenticate using Application Default Credentials (ADC)",
        "i" = "Run: {.code gcloud auth application-default login}",
        "i" = "Or configure service account in container environment",
        "!" = "Error: {conditionMessage(e)}"
      ))
    }
  )
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
    if (grepl("execute_req.R|renv|driver_ca.R|dependencies.R", file)) {
      next
    } else {
      echo_text(paste0("sourcing : ", file))
      source(file)
    }
  }
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

clean_up <- function(db = get_argos_default()$config("db_src")) {
  if (class(db)[1] %in% c("Oracle", "src_BigQueryConnection", "Spark SQL")) {
    get_argos_default()$.__enclos_env__$private$.ora_bq_db_env_cleanup(db = db)
  }

  if (!class(db)[1] == "src_BigQueryConnection") {
    DBI::dbDisconnect(conn = db)
  }
}

init_banner <- function(query_title) {
  cli::cli_h1("")
  if (.Platform$OS.type == "windows") {
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
  cli:::cli_h1("Parameterized Advanced Querying System (PAQS) v1.0")
  cli::cli_text("\n\n")
  cli:::cli_h1(query_title)
  cli::cli_h1("")
  cli::cli_text("\n\n")
  Sys.sleep(2)
}


###################################################################################################
#' `Patches to srcr for interactive input as well as modifying session specific argos functions` ##
#' *DO NOT EDIT*	                                                                               ##
###################################################################################################

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

patch_srcr <- function() {
  utils::assignInNamespace(
    x = "srcr",
    value = function(
      basenames = NA,
      dirs = NA,
      suffices = NA,
      paths = NA,
      config = NA,
      allow_post_connect = getOption("srcr.allow_post_connect", c())
    ) {
      if (is.na(config)) {
        if (is.na(paths)) {
          args <- mget(c("dirs", "basenames", "suffices"))
          args <- args[!is.na(args)]
          paths <- do.call(srcr::find_config_files, args)
          if (length(paths) < 1) {
            stop(
              "No config files found for ",
              paste(
                vapply(
                  names(args),
                  function(x) {
                    paste(
                      x,
                      "=",
                      paste(args[[x]], collapse = ", ")
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
        
        # Helper function to get password from environment (case-insensitive)
        .get_env_password <- function() {
          # Try common case variations
          for (var_name in c('db_pwd')) {
            pwd <- Sys.getenv(var_name, unset = "")
            if (nzchar(pwd)) {
              return(pwd)
            }
          }
          return("")
        }
        
        if (config$src_name == "Postgres") {
          # Check for environment variable first, then prompt if not found
          env_password <- .get_env_password()
          if (nzchar(env_password)) {
            config$src_args$password <- env_password
          } else {
            config$src_args$password <- getPass::getPass(
              msg = paste0("Enter Password for ", config$src_args$user, " : ")
            )
          }
        }
        
        if (config$src_name == "odbc") {
          if (!is.null(config$src_args$Trusted_Connection)) {
            if (config$src_args$Trusted_Connection == "yes") {
              config$src_args$UID <- NULL
              config$src_args$PWD <- NULL
            } else {
              config$src_args$Trusted_Connection <- NULL
              # Check for environment variable first
              env_password <- .get_env_password()
              if (nzchar(env_password)) {
                config$src_args$PWD <- env_password
              } else {
                config$src_args$PWD <- getPass::getPass(
                  msg = paste0("Enter Password for ", config$src_args$UID, " : ")
                )
              }
            }
          } else {
            # Check for environment variable first
            env_password <- .get_env_password()
            if (nzchar(env_password)) {
              config$src_args$PWD <- env_password
            } else {
              config$src_args$PWD <- getPass::getPass(
                msg = paste0("Enter Password for ", config$src_args$UID, " : ")
              )
            }
          }
        }
      }
      if (!grepl("^src_", config$src_name)) {
        drv <- tryCatch(DBI::dbDriver(config$src_name), error = function(e) {
          NULL
        })
        if (is.null(drv)) {
          lib <- tryCatch(
            library(config$src_name, character.only = TRUE),
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

      if (
        any(allow_post_connect == "sql") &&
          exists("post_connect_sql", where = config)
      ) {
        con <- if (inherits(db, "src_dbi")) db$con else db
        lapply(config$post_connect_sql, function(x) DBI::dbExecute(con, x))
      }
      if (
        any(allow_post_connect == "fun") &&
          exists("post_connect_fun", where = config)
      ) {
        pc <- eval(parse(
          text = paste(config$post_connect_fun, sep = "\n", collapse = "\n")
        ))
        db <- do.call(pc, list(db))
      }
      db
    },
    ns = "srcr"
  )
}

patch_argos <- function() {
  argos$public_methods$load_codeset <- function(
    name,
    col_types = "iccc",
    table_name = name,
    indexes = list("concept_code"),
    full_path = FALSE,
    db = self$config("db_src")
  ) {
    conn_class <- class(db)[1]
    method_name <- paste0("load_codeset.", conn_class)

    if (method_name %in% names(self)) {
      return(self[[method_name]](
        name,
        col_types,
        table_name,
        indexes,
        full_path,
        db
      ))
    } else {
      return(self$`load_codeset.default`(
        name,
        col_types,
        table_name,
        indexes,
        full_path,
        db
      ))
    }
  }

  argos$public_methods$`load_codeset.default` <- function(
    name,
    col_types = "iccc",
    table_name = name,
    indexes = list("concept_code"),
    full_path = FALSE,
    db = self$config("db_src")
  ) {
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

  argos$public_methods$`load_codeset.Oracle` <- function(
    name,
    col_types = "iccc",
    table_name = name,
    indexes = list("concept_code"),
    full_path = FALSE,
    db = self$config("db_src")
  ) {
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
      df = self$read_codeset(
        name,
        col_types = col_types,
        full_path = full_path
      ),
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

  argos$public_methods$`load_codeset.src_BigQueryConnection` <- function(
    name,
    col_types = "iccc",
    table_name = name,
    indexes = list("concept_code"),
    full_path = FALSE,
    db = self$config("db_src")
  ) {
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
      cli::cli_alert_info(
        "Creating codeset table '{name}' in project '{codeset_project}', dataset '{codeset_dataset}'"
      )

      # Create a BigQuery table reference with explicit project and dataset
      bq_conn <- bigrquery::bq_project_query(codeset_project, "SELECT 1")
      table_id <- bigrquery::bq_table(
        project = codeset_project,
        dataset = codeset_dataset,
        table = name
      )

      # Delete the table if it exists (for overwrite)
      if (bigrquery::bq_table_exists(table_id)) {
        bigrquery::bq_table_delete(table_id)
      }

      # Upload data to BigQuery table

      df <- self$read_codeset(
        name,
        col_types = col_types,
        full_path = full_path
      )
      fields <- as_bq_fields(df)
      # Upload data to BigQuery table
      bigrquery::bq_table_upload(
        table_id,
        df,
        fields = fields,
        write_disposition = "WRITE_TRUNCATE"
      )

      # Create a dplyr reference to the table with explicit project.dataset.table format
      qualified_table_name <- paste0(
        "`",
        codeset_project,
        ".",
        codeset_dataset,
        ".",
        name,
        "`"
      )
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

  argos$public_methods$compute_new <- function(
    tblx,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    conn_class <- class(get_argos_default()$config('db_src'))[1]
    method_name <- paste0("compute_new.", conn_class)

    if (method_name %in% names(self)) {
      return(self[[method_name]](tblx, name, temporary, ...))
    } else {
      return(self$`compute_new.default`(tblx, name, temporary, ...))
    }
  }

  argos$public_methods$`compute_new.default` <- function(
    tblx,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    if (!inherits(name, c("ident_q", "dbplyr_schema")) && length(name) == 1) {
      name <- gsub("\\s+", "_", name, perl = TRUE)
      name <- self$intermed_name(name, temporary)
    }
    con <- self$dbi_con(tblx)
    #' Apperantly MS SQL pads temperory table name with `#` so we modify the name
    #' to make sure that following db_exists_new and db_remove_new logic doesn't fail.

    # Broken in dbplyr, so do it ourselves
    if (self$db_exists_table(con, name)) {
      self$db_remove_table(con, name)
    }

    if (self$config("db_trace")) {
      show_query(tblx)
      if (self$config("can_explain")) {
        explain(tblx)
      }
      message(
        " -> ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(name, con)
        )
      )
      start <- Sys.time()
      message(start)
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    rslt <-
      do.call(
        dplyr::compute,
        c(list(tblx, name = name, temporary = temporary), ellipsis_args)
      )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`compute_new.Microsoft SQL Server` <- function(
    tblx,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    if (!inherits(name, c("ident_q", "dbplyr_schema")) && length(name) == 1) {
      name <- gsub("\\s+", "_", name, perl = TRUE)
      name <- self$intermed_name(name, temporary)
    }
    con <- self$dbi_con(tblx)

    if (temporary) {
      if (self$db_exists_table(con, paste0("#", name))) {
        self$db_remove_table(con, paste0("#", name))
      }
    }

    if (self$config("db_trace")) {
      show_query(tblx)
      if (self$config("can_explain")) {
        explain(tblx)
      }
      message(
        " -> ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(name, con)
        )
      )
      start <- Sys.time()
      message(start)
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    rslt <- do.call(
      dplyr::compute,
      c(list(tblx, name = name, temporary = temporary), ellipsis_args)
    )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`compute_new.Oracle` <- function(
    tblx,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    if (!inherits(name, c("ident_q", "dbplyr_schema")) && length(name) == 1) {
      name <- gsub("\\s+", "_", name, perl = TRUE)
      name <- self$intermed_name(name, temporary)
    }
    con <- self$dbi_con(tblx)

    if (self$config("db_trace")) {
      show_query(tblx)
      if (self$config("can_explain")) {
        explain(tblx)
      }
      message(
        " -> ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(name, con)
        )
      )
      start <- Sys.time()
      message(start)
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    self$config(
      "temp_table_drop_me",
      append(self$config("temp_table_drop_me"), name)
    )

    rslt <- do.call(
      dplyr::copy_to,
      c(
        list(
          con,
          tblx,
          name = dbplyr::in_schema(self$config("temp_table_schema"), name),
          overwrite = TRUE,
          temporary = FALSE
        ),
        ellipsis_args
      )
    )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`compute_new.Spark SQL` <- function(
    tblx,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    temporary = !self$config("retain_intermediates"),
    overwrite = TRUE,
    ...
  ) {
    if (!inherits(name, c("ident_q", "dbplyr_schema")) && length(name) == 1) {
      name <- gsub("\\s+", "_", name, perl = TRUE)
      name <- self$intermed_name(name, temporary)
    }
    con <- self$dbi_con(tblx)

    # Check if we need to rotate schema (only in local_test mode)
    if (self$config_exists("local_test")) {
      if (self$config("local_test")) {
        current_schema <- self$config("temp_table_schema")

        # Query actual table count from the schema
        tryCatch(
          {
            table_count_query <- paste0(
              "SELECT COUNT(*) as table_count FROM ",
              current_schema,
              ".information_schema.tables WHERE table_schema = '",
              current_schema,
              "'"
            )

            # Alternative query for Databricks (more reliable)
            table_count_query <- paste0(
              "SHOW TABLES IN ",
              current_schema
            )

            result <- DBI::dbGetQuery(con, table_count_query)
            current_schema_count <- nrow(result %>% filter(!isTemporary))

            cli_alert_info(paste0(
              "Current Schema : `",
              current_schema,
              "` | ",
              "Total tables written : ",
              current_schema_count
            ))

            # If we've hit 90 tables, rotate to a new schema
            if (current_schema_count >= 90) {
              # Generate new schema name
              current_schema <- self$config('temp_table_schema')
              available_schema <- self$config('all_available_schemas')

              schema_num <- which(current_schema == available_schema)

              if (is.na(schema_num)) {
                schema_num <- 1
              } else {
                schema_num <- schema_num + 1
              }
              new_schema <- available_schema[schema_num]

              if (is.na(new_schema)) {
                cli_abort(
                  "Available schemas doesn't exist on the catalog or the current schema list is exhausted, ask admins to add a new schema to continue execution"
                )
              }

              cli_alert_info(paste0(
                "Rotating to new schema: ",
                new_schema,
                " (current schema has ",
                current_schema_count,
                " tables)"
              ))

              self$config("temp_table_schema", new_schema)
            }
          },
          error = function(e) {
            warning(
              "Could not query table count for schema rotation: ",
              e$message
            )
          }
        )
      }
    }

    if (self$config("db_trace")) {
      show_query(tblx)
      if (self$config("can_explain")) {
        explain(tblx)
      }
      message(
        " -> ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(name, con)
        )
      )
      start <- Sys.time()
      message(start)
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    sql <- dbplyr::sql_render(tblx)

    # Create schema-qualified name object for proper tracking
    schema_qualified_name <- DBI::Id(
      schema = self$config("temp_table_schema"),
      table = name
    )

    # Store the schema-qualified name for cleanup
    self$config(
      "temp_table_drop_me",
      append(self$config("temp_table_drop_me"), list(schema_qualified_name))
    )

    quoted_name <- DBI::dbQuoteIdentifier(con, schema_qualified_name)

    sql_statement <- paste0(
      "CREATE ",
      if (overwrite) "OR REPLACE " else "",
      "TABLE ",
      quoted_name,
      " AS\n",
      sql
    )

    DBI::dbExecute(con, sql_statement)

    rslt <- tbl(
      con,
      in_schema(schema = self$config('temp_table_schema'), table = name)
    )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$copy_to_new <- function(
    dest = self$config("db_src"),
    df,
    name = deparse(substitute(df)),
    overwrite = TRUE,
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    conn_class <- class(get_argos_default()$config('db_src'))[1]
    method_name <- paste0("copy_to_new.", conn_class)

    if (method_name %in% names(self)) {
      return(self[[method_name]](dest, df, name, overwrite, temporary, ...))
    } else {
      return(self$`copy_to_new.default`(
        dest,
        df,
        name,
        overwrite,
        temporary,
        ...
      ))
    }
  }

  argos$public_methods$`copy_to_new.default` <- function(
    dest = self$config("db_src"),
    df,
    name = deparse(substitute(df)),
    overwrite = TRUE,
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    name <- self$intermed_name(name, temporary = temporary)
    if (self$config("db_trace")) {
      message(" -> copy_to")
      start <- Sys.time()
      message(start)
      message("Data: ", deparse(substitute(df)))
      message(
        "Table name: ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(
            name,
            dbi_con(dest)
          )
        ),
        " (temp: ",
        temporary,
        ")"
      )
      message("Data elements: ", paste(tbl_vars(df), collapse = ","))
      message("Rows: ", NROW(df))
    }
    if (
      class(self$config("db_src"))[1] == "Microsoft SQL Server" && temporary
    ) {
      if (overwrite && self$db_exists_table(dest, paste0("#", name))) {
        self$db_remove_table(dest, paste0("#", name))
      }
    } else {
      if (overwrite && self$db_exists_table(dest, name)) {
        self$db_remove_table(dest, name)
      }
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    rslt <- do.call(
      dplyr::copy_to,
      (c(
        list(
          dest = dest,
          df = df,
          name = name,
          overwrite = overwrite,
          temporary = temporary
        ),
        ellipsis_args
      ))
    )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`copy_to_new.Microsoft SQL Server` <- function(
    dest = self$config("db_src"),
    df,
    name = deparse(substitute(df)),
    overwrite = TRUE,
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    name <- self$intermed_name(name, temporary = temporary)
    if (self$config("db_trace")) {
      message(" -> copy_to")
      start <- Sys.time()
      message(start)
      message("Data: ", deparse(substitute(df)))
      message(
        "Table name: ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(
            name,
            dbi_con(dest)
          )
        ),
        " (temp: ",
        temporary,
        ")"
      )
      message("Data elements: ", paste(tbl_vars(df), collapse = ","))
      message("Rows: ", NROW(df))
    }

    if (temporary) {
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
    # Really not sure where this is coming from on windows server but we'll have to troubleshoot later at some point.

    ellipsis_args$.chunk_size = c()

    rslt <- do.call(
      dplyr::copy_to,
      (c(
        list(
          dest = dest,
          df = df,
          name = name,
          overwrite = overwrite,
          temporary = temporary
        ),
        ellipsis_args
      ))
    )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`copy_to_new.Oracle` <- function(
    dest = self$config("db_src"),
    df,
    name = deparse(substitute(df)),
    overwrite = TRUE,
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    name <- self$intermed_name(name, temporary = temporary)
    if (self$config("db_trace")) {
      message(" -> copy_to")
      start <- Sys.time()
      message(start)
      message("Data: ", deparse(substitute(df)))
      message(
        "Table name: ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(
            name,
            dbi_con(dest)
          )
        ),
        " (temp: ",
        temporary,
        ")"
      )
      message("Data elements: ", paste(tbl_vars(df), collapse = ","))
      message("Rows: ", NROW(df))
    }

    if (overwrite && self$db_exists_table(dest, name)) {
      self$db_remove_table(dest, name)
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    self$config(
      "temp_table_drop_me",
      append(self$config("temp_table_drop_me"), name)
    )

    rslt <-
      do.call(
        dplyr::copy_to,
        c(
          list(
            dest,
            df,
            name = dbplyr::in_schema(self$config("temp_table_schema"), name),
            overwrite = TRUE,
            temporary = FALSE
          ),
          ellipsis_args
        )
      )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`copy_to_new.Spark SQL` <- function(
    dest = self$config("db_src"),
    df,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    overwrite = TRUE,
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    name <- paste0(sample(letters, 12, replace = TRUE), collapse = "")
    if (self$config("db_trace")) {
      message(" -> copy_to")
      start <- Sys.time()
      message(start)
      message("Data: ", deparse(substitute(df)))
      message(
        "Table name: ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(
            name,
            dbi_con(dest)
          )
        ),
        " (temp: ",
        temporary,
        ")"
      )
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
    ellipsis_args$.chunk_size = c()
    if (self$config_exists("can_index")) {
      if (!self$config("can_index")) {
        ellipsis_args$indexes <- c()
      }
    }

    rslt <- do.call(
      dplyr::copy_to,
      (c(
        list(
          dest = dest,
          df = df,
          name = name,
          overwrite = overwrite,
          temporary = temporary
        ),
        ellipsis_args
      ))
    )

    if (self$config("db_trace")) {
      end <- Sys.time()
      message(end, " ==> ", format(end - start))
    }
    rslt
  }

  argos$public_methods$`copy_to_new.src_BigQueryConnection` <- function(
    dest = self$config("db_src"),
    df,
    name = paste0(sample(letters, 12, replace = TRUE), collapse = ""),
    overwrite = TRUE,
    temporary = !self$config("retain_intermediates"),
    ...
  ) {
    name <- paste0(sample(letters, 12, replace = TRUE), collapse = "")
    if (self$config("db_trace")) {
      message(" -> copy_to")
      start <- Sys.time()
      message(start)
      message("Data: ", deparse(substitute(df)))
      message(
        "Table name: ",
        base::ifelse(
          packageVersion("dbplyr") < "2.0.0",
          dbplyr::as.sql(name),
          dbplyr::as.sql(
            name,
            dbi_con(dest)
          )
        ),
        " (temp: ",
        temporary,
        ")"
      )
      message("Data elements: ", paste(tbl_vars(df), collapse = ","))
      message("Rows: ", NROW(df))
    }

    ellipsis_args <- list(...)
    ellipsis_args$.chunk_size = c()
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
      cli::cli_alert_info(
        "Loading  table '{name}' in project '{codeset_project}', dataset '{codeset_dataset}'"
      )

      # Create a BigQuery table reference with explicit project and dataset
      bq_conn <- bigrquery::bq_project_query(codeset_project, "SELECT 1")
      table_id <- bigrquery::bq_table(
        project = codeset_project,
        dataset = codeset_dataset,
        table = name
      )

      # Delete the table if it exists (for overwrite)
      if (bigrquery::bq_table_exists(table_id)) {
        bigrquery::bq_table_delete(table_id)
      }

      fields <- as_bq_fields(df)

      bigrquery::bq_table_upload(
        table_id,
        df,
        fields = fields,
        write_disposition = "WRITE_TRUNCATE"
      )

      qualified_table_name <- paste0(
        "`",
        codeset_project,
        ".",
        codeset_dataset,
        ".",
        name,
        "`"
      )

      sql_query <- paste0("SELECT * FROM ", qualified_table_name, " as q")
      rslt <- dplyr::tbl(dest, dplyr::sql(sql_query))

      if (self$config("db_trace")) {
        end <- Sys.time()
        message(end, " ==> ", format(end - start))
      }
    }
    rslt
  }

  argos$private_methods$.ora_env_setup <-
    function(db) {
      if (class(db)[1] == "Oracle") {
        options(odbc.batch_rows = 1)
        DBI::dbExecute(db, "alter session set nls_date_format = 'YYYY-MM-DD'")
        DBI::dbExecute(
          db,
          "alter session set nls_timestamp_tz_format = 'YYYY-MM-DD HH24:MI:SS TZHTZM'"
        )
      }
    }

  argos$private_methods$.ora_bq_db_env_cleanup <- function(db) {
    dm <- self$config("temp_table_drop_me")
    if (length(dm) == 0) {
      return(invisible(TRUE))
    }

    vapply(
      unique(dm),
      function(t) {
        if (class(db)[1] == "Oracle") {
          tryCatch(
            {
              get_argos_default()$db_remove_table(db, t)
              cli::cli_alert_success("Removed table {.val {t}}")
              1L
            },
            error = function(e) {
              message(t, " - ", e)
              0L
            }
          )
        } else if (class(db)[1] == "src_BigQueryConnection") {
          tryCatch(
            {
              if (
                bigrquery::bq_table_exists(bigrquery::bq_table(
                  project = self$config('codeset_project'),
                  dataset = self$config('codeset_dataset_name'),
                  table = t
                ))
              ) {
                bigrquery::bq_table_delete(bigrquery::bq_table(
                  project = self$config('codeset_project'),
                  dataset = self$config('codeset_dataset_name'),
                  table = t
                ))
                cli::cli_alert_success("Removed table {.val {t}}")
                1L
              }
            },
            error = function(e) {
              message(t, " - ", e)
              0L
            }
          )
        } else if (class(db)[1] == "Spark SQL") {
          tryCatch(
            {
              dbRemoveTable(
                db,
                t
              )
              cli::cli_alert_success(
                "Removed table {.val {DBI::dbQuoteIdentifier(db, t)}}"
              )
              1L
            },
            error = function(e) {
              message(t, " - ", e)
              0L
            }
          )
        }
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
        name <- base::ifelse(
          self$config("cdm_case") == "upper",
          toupper(name),
          name
        )
      }
      if (!is.na(schema_tag)) {
        if (self$config_exists(schema_tag)) {
          schema_tag <- self$config(schema_tag)
        }
        if (!is.na(schema_tag)) {
          if (packageVersion("dbplyr") < "2.0.0") {
            # nocov start
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
    self$tbl_case_corrector(self$qual_tbl(name, "cdm_schema", db)) %>%
      select(-contains('site'))
  }
}


# Modifying pkgload to ensure version of pacakges for the importatant and basic ones where renv fails.

pkgLoad <- function() {
  if (is.na(match("pak", utils::installed.packages()[, 1]))) {
    install.packages("pak")
  }

  if (is.na(match("pkgdepends", utils::installed.packages()[, 1]))) {
    install.packages("pkgdepends")
  }

  # Define required package versions
  required_versions <- list(
    "DBI" = "1.2.3",
    "odbc" = "1.5.0",
    "ggplot" = "4.0.0",
    "dplyr" = "1.1.4",
    "dbplyr" = "2.5.0",
    "tidyverse" = "2.0.0"
  )

  req_packages <- list(
    CRAN = c(
      "getPass",
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
      "duckdb",
      "quarto",
      "bigrquery",
      "dplyr",
      "dbplyr",
      "pkgdepends",
      "glue"
    ),
    github = c(
      "expectedvariablespresent",
      "conceptsetdistribution",
      "clinicalevents.specialties",
      "patienteventsequencing",
      "sensitivityselectioncriteria",
      "patientrecordconsistency",
      "squba.gen",
      "cohortattrition",
      "argos"
    )
  )

  # Function to check if package version exactly matches requirements
  check_version <- function(pkg_name, required_version) {
    installed_pkgs <- utils::installed.packages()
    if (pkg_name %in% installed_pkgs[, "Package"]) {
      installed_version <- installed_pkgs[pkg_name, "Version"]
      return(utils::compareVersion(installed_version, required_version) == 0)
    }
    return(FALSE)
  }

  # Check which packages need installation/update
  packagecheck <- lapply(req_packages, function(package_list) {
    sapply(package_list, function(pkg) {
      if (pkg %in% names(required_versions)) {
        # Check version for packages with specific version requirements
        if (check_version(pkg, required_versions[[pkg]])) {
          return(1) # Package exists with correct version
        } else {
          return(NA) # Package needs installation/update
        }
      } else {
        # For packages without version requirements, just check if installed
        match(pkg, utils::installed.packages()[, 1])
      }
    })
  })

  # Determine packages to install
  to_install <- lapply(names(req_packages), function(source) {
    pkg_list <- req_packages[[source]]
    check_result <- packagecheck[[source]]
    pkg_list[is.na(check_result)]
  })

  names(to_install) <- names(req_packages)
  to_install <- to_install[sapply(to_install, length) > 0]

  if (length(to_install) > 0L) {
    for (source in names(to_install)) {
      if (source == "CRAN") {
        for (pkg in to_install[[source]]) {
          if (pkg %in% names(required_versions)) {
            # Install specific version
            version_spec <- paste0(pkg, "@", required_versions[[pkg]])
            message(paste("Installing", version_spec))
            pak::pkg_install(version_spec, dependencies = TRUE)
          } else {
            # Install latest version
            pak::pkg_install(pkg, dependencies = TRUE)
          }
        }
      }

      if (source == "github") {
        for (pkg in to_install[[source]]) {
          if (pkg != "argos") {
            pak::pkg_install(paste0("ssdqa/", pkg), dependencies = TRUE)
          } else {
            pak::pkg_install(paste0("PEDSnet/", pkg), dependencies = TRUE)
          }
        }
      }
    }
  } else {
    message("All requested packages already installed with correct versions")
  }

  # Load packages
  for (i in seq_along(req_packages)) {
    for (j in req_packages[[i]]) {
      suppressPackageStartupMessages(library(
        j,
        character.only = TRUE,
        quietly = TRUE
      ))
    }
  }

  # Verify versions after loading
  message("Verifying installed versions:")
  for (pkg in names(required_versions)) {
    if (pkg %in% loadedNamespaces()) {
      installed_version <- utils::packageVersion(pkg)
      required_version <- required_versions[[pkg]]
      status <- if (
        utils::compareVersion(
          as.character(installed_version),
          required_version
        ) ==
          0
      ) {
        "✓"
      } else {
        "✗"
      }
      message(paste(
        status,
        pkg,
        installed_version,
        "(required:",
        required_version,
        ")"
      ))
    }
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

patch_squba <- function() {
  utils::assignInNamespace(
    x = "calc_days_between_dates",
    value = function(
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
        sql_code <- paste0(
          'TRUNC("',
          date_col_2,
          '") - TRUNC("',
          date_col_1,
          '")'
        )
      } else if (class(db)[1] == "src_BigQueryConnection") {
        sql_code <- paste0("DATE_DIFF(", date_col_2, ", ", date_col_1, ", DAY)")
      } else if (class(db) %in% 'SQLiteConnection') {
        sql_code <-
          paste0("julianday(", date_col_2, ") - julianday(", date_col_1, ")")
      } else if (class(db) %in% 'PrestoConnection') {
        sql_code <-
          paste0("date_diff(day, ", date_col_1, ", ", date_col_2, ")")
      } else if (class(db)[1] %in% 'Spark SQL') {
        sql_code <-
          paste0("datediff(", date_col_2, ",", date_col_1, ")")
      } else {
        sql_code <-
          paste0(date_col_2, "::date", " - ", date_col_1, "::date")
      }
      return(sql_code)
    },
    ns = "squba.gen"
  )

  utils::assignInNamespace(
    x = "compute_demographic_summary_pcnt",
    value = function(
      cohort_tbl,
      site_col,
      person_tbl = cdm_tbl('demographic'),
      visit_tbl = cdm_tbl('encounter'),
      demographic_mappings = sensitivityselectioncriteria::ssc_pcornet_demographics
    ) {
      demo_list <- split(demographic_mappings, seq(nrow(demographic_mappings)))
      demo_rslt <- list()

      for (i in 1:length(demo_list)) {
        vals <- demo_list[[i]]$field_values %>%
          str_replace_all(" ", "") %>%
          str_split(',') %>%
          unlist()

        demographic <- person_tbl %>%
          inner_join(cohort_tbl) %>%
          mutate(
            demo_col = ifelse(
              !!sym(demo_list[[i]]$concept_field) %in% vals,
              1,
              0
            )
          ) %>%
          select(
            !!sym(site_col),
            patid,
            start_date,
            end_date,
            fu,
            cohort_id,
            demo_col
          ) %>%
          rename(!!sym(demo_list[[i]]$demographic) := demo_col) %>%
          collect()

        demo_rslt[[i]] <- demographic
      }

      demo_final <- purrr::reduce(.x = demo_rslt, .f = left_join)
      #' SM Edits
      #' `Explitely casting as.Date type to ensure that age computation doesn't fail silently`
      age_ced <- person_tbl %>%
        inner_join(cohort_tbl) %>%
        collect() %>%
        mutate(
          age_cohort_entry = as.numeric(
            as.Date(start_date) - as.Date(birth_date)
          ),
          age_cohort_entry = round(age_cohort_entry / 365.25, 2)
        ) %>%
        distinct(!!sym(site_col), patid, age_cohort_entry)

      age_first_visit <- person_tbl %>%
        inner_join(cohort_tbl) %>%
        inner_join(select(visit_tbl, patid, admit_date)) %>%
        group_by(!!sym(site_col), patid, cohort_id, birth_date) %>%
        summarise(min_visit = min(admit_date)) %>%
        collect() %>%
        mutate(
          age_first_visit = as.numeric(
            as.Date(min_visit) - as.Date(birth_date)
          ),
          age_first_visit = round(age_first_visit / 365.25, 2)
        ) %>%
        select(-c(min_visit, birth_date)) #%>%
      #collect()

      summ_tbl <- demo_final %>%
        left_join(age_first_visit) %>%
        left_join(age_ced)
    },
    ns = "sensitivityselectioncriteria"
  )

  utils::assignInNamespace(
    x = "prepare_cohort",
    function(cohort_tbl, age_groups = NULL, codeset = NULL, omop_or_pcornet) {
      ct <- get_argos_default()$copy_to_new(
        df = cohort_tbl,
        name = paste0(sample(letters, 12, replace = TRUE), collapse = "")
      )

      stnd <-
        ct %>%
        mutate(
          fu_diff = sql(calc_days_between_dates(
            date_col_1 = 'start_date',
            date_col_2 = 'end_date'
          )),
          fu_diff = as.numeric(fu_diff),
          fu = round(fu_diff / 365.25, 3)
        )

      if (!is.data.frame(age_groups)) {
        final_age <- stnd
      } else {
        if (omop_or_pcornet == 'omop') {
          final_age <- compute_age_groups_omop(
            cohort_tbl = stnd,
            person_tbl = cdm_tbl('person'),
            age_groups = age_groups
          )
        } else if (omop_or_pcornet == 'pcornet') {
          final_age <- compute_age_groups_pcnt(
            cohort_tbl = stnd,
            person_tbl = cdm_tbl('demographic'),
            age_groups = age_groups
          )
        }
      }

      # if(!is.data.frame(codeset)){
      #   final_cdst <- stnd
      # }else{
      #   if(omop_or_pcornet == 'omop'){
      #   final_cdst <- cohort_codeset_label_omop(cohort_tbl = stnd,
      #                                           codeset_meta = codeset)
      #   }else if(omop_or_pcornet == 'pcornet'){
      #     final_cdst <- cohort_codeset_label_pcnt(cohort_tbl = stnd,
      #                                             codeset_meta = codeset)
      #   }
      # }

      final <- stnd %>%
        left_join(final_age) #%>%
      #left_join(final_cdst)

      return(final)
    },
    ns = "squba.gen"
  )
}

calc_days_between_dates <-
  function(date_col_1, date_col_2, db = get_argos_default()$config("db_src")) {
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
      sql_code <- paste0(
        'TRUNC("',
        date_col_2,
        '") - TRUNC("',
        date_col_1,
        '")'
      )
    } else if (class(db)[1] == "src_BigQueryConnection") {
      sql_code <- paste0("DATE_DIFF(", date_col_2, ", ", date_col_1, ", DAY)")
    } else if (class(db) %in% 'SQLiteConnection') {
      sql_code <-
        paste0("julianday(", date_col_2, ") - julianday(", date_col_1, ")")
    } else if (class(db) %in% 'PrestoConnection') {
      sql_code <-
        paste0("date_diff(day, ", date_col_1, ", ", date_col_2, ")")
    } else if (class(db)[1] %in% 'Spark SQL') {
      sql_code <-
        paste0("datediff(", date_col_2, ",", date_col_1, ")")
    } else {
      sql_code <-
        paste0(date_col_2, "::date", " - ", date_col_1, "::date")
    }
    return(sql_code)
  }


add_days_to_date <-
  function(date_col, days_to_add, db = get_argos_default()$config("db_src")) {
    if (class(db)[1] == "Microsoft SQL Server") {
      sql_code <-
        paste0("DATEADD(day, ", days_to_add, ", ", date_col, ")")
    } else if (class(db)[1] == "Snowflake") {
      sql_code <-
        paste0("DATEADD(day, ", days_to_add, ", ", date_col, ")")
    } else if (class(db)[1] == "Oracle") {
      sql_code <- paste0('("', date_col, '" + ', days_to_add, ')')
    } else if (class(db)[1] == "src_BigQueryConnection") {
      sql_code <- paste0(
        "DATE_ADD(",
        date_col,
        ", INTERVAL ",
        days_to_add,
        " DAY)"
      )
    } else if (class(db) %in% 'SQLiteConnection') {
      sign <- if (days_to_add >= 0) "+" else ""
      sql_code <-
        paste0("date(", date_col, ", '", sign, days_to_add, " days')")
    } else if (class(db) %in% 'PrestoConnection') {
      sql_code <-
        paste0("date_add('day', ", days_to_add, ", ", date_col, ")")
    } else if (class(db)[1] %in% 'Spark SQL') {
      sql_code <-
        paste0("date_add(", date_col, ", ", days_to_add, ")")
    } else {
      sql_code <-
        paste0(date_col, "::date + INTERVAL '", days_to_add, " days'")
    }
    return(sql_code)
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


if (requireNamespace("getPass", quietly = TRUE)) {
  utils::assignInNamespace(
    x = "getPass",
    value = function(msg = "PASSWORD: ", noblank = FALSE, forcemask = FALSE) {
      if (!is.character(msg) || length(msg) != 1 || is.na(msg)) {
        stop("argument 'msg' must be a single string")
      }
      if (!is.logical(noblank) || length(noblank) != 1 || is.na(noblank)) {
        stop("argument 'noblank' must be one of 'TRUE' or 'FALSE'")
      }
      if (
        !is.logical(forcemask) || length(forcemask) != 1 || is.na(forcemask)
      ) {
        stop("argument 'forcemask' must be one of 'TRUE' or 'FALSE'")
      }
      if (tolower(.Platform$GUI) == "rstudio" || Sys.getenv('POSITRON') == 1) {
        pw <- getPass:::readline_masked_rstudio(
          msg = msg,
          noblank = noblank,
          forcemask = forcemask
        )
      } else if (getPass:::isaterm()) {
        pw <- getPass:::readline_masked_term(
          msg = msg,
          showstars = TRUE,
          noblank = noblank
        )
      } else if (getPass:::os_windows()) {
        pw <- getPass:::readline_masked_wincred(msg = msg, noblank = noblank)
      } else if (getPass:::hastcltk()) {
        pw <- getPass:::readline_masked_tcltk(msg = msg, noblank = noblank)
      } else if (!forcemask) {
        pw <- getPass:::readline_nomask(msg, noblank = noblank)
      } else {
        stop("Masking is not supported on your platform!")
      }
      if (is.null(pw)) {
        invisible()
      } else {
        pw
      }
    },
    ns = "getPass"
  )
}
