load_all_codesets <- function(path = paste0('./query/',get_argos_default()$config('subdirs')$spec_dir)) {

  files <- gsub(".csv","",list.files(path, pattern = 'csv'))

  codesets <- setNames(
  lapply(files, load_codeset, col_types = "ccc", indexes = "code"),
  basename(files)
  )
  
codesets
}