

drop_temp_table <- function(df_list) {
  
  for (i in names(df_list)) {
    
    temp_tbl_name <- gsub('"', '', as.character(df_list[[paste0(i)]]$lazy_query$x))
    
    if (!class(get_argos_default()$config('db_src'))[1] %in% c("src_BigQueryConnection", "Spark SQL")) {
      if (db_exists_table(name = temp_tbl_name)) {
        db_remove_table(name = temp_tbl_name)
        echo_text(glue("dropped {temp_tbl_name}"))
      }
    }
  }
}