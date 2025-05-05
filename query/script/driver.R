#' Execute the request
#'
#' This function presumes the environment has been set up, and executes the
#' steps of the request.
#'
#' In addition to performing queries and analyses, the execution path in this
#' function should include periodic progress messages to the user, and logging
#' of intermediate totals and timing data through [append_sum()].
#'
#' @return The return value is dependent on the content of the request, but is
#'   typically a structure pointing to some or all of the retrieved data or
#'   analysis results.  The value is not used by the framework itself.
#' @md
run <- function() {
  # ===================================================================================================
  #' **Standard code DO NOT EDIT | Fill values where necessary**
  # ===================================================================================================
  # init_progress_bar(total_steps = 13)

  init_message(query_title = "")

  .GlobalEnv$total_steps <- 0L
  .GlobalEnv$query_name <- ""

  .GlobalEnv$test_stat <- init_sum(Test = "Start of Query", Status = "", set_default = NULL)
  logFile <- file(file.path(
    get_argos_default()$config("base_dir"),
    get_argos_default()$config("subdirs")$result_dir,
    paste0(.GlobalEnv$query_name,".log")
  ), open = "wt")

  sink(file = logFile, type = "message", append = TRUE)
  sink(file = logFile, type = "output", append = TRUE)

  get_argos_default()$config('temp_table_drop_me', character(0))
  get_argos_default()$.__enclos_env__$private$.ora_env_setup(db = get_argos_default()$config('db_src'))
  # ===================================================================================================
  #' **WRITE YOUR QUERY FROM HERE ->**
  # ===================================================================================================
  # Adding a list object to store results on the global environment ====

  rslt <- list()



  # ===================================================================================================
  #' **Standard code DO NOT EDIT**
  # ===================================================================================================
  sink(type = "output")
  sink(type = "message")

  close.connection(logFile)

  on.exit(exit())

if (Sys.getenv("native_execution") != "") {
  if (!as.logical(Sys.getenv("native_execution"))) {
    system("quarto render query/script/report.qmd --output-dir ../results/ --execute-dir query/results/ --to html")
  } else {
    quarto::quarto_render("query/script/report.qmd", execute_dir = "query/results/")
    file.rename(paste0("query/script/",.GlobalEnv$query_name,"_report.html"), paste0("query/results/",.GlobalEnv$query_name,"_report.html"))
  }
} else {
  system("quarto render query/script/report.qmd --output-dir ../results/ --execute-dir query/results/ --to html")
}

}
