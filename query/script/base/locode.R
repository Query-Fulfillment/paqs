if(all(pull(codesets$dx_any_icd_10, code) == "*")) {
  
echo_text('Wild card entry detected')

codetype <- codeset %>% pull(codetype)
 
cdm_tbl(table_name) %>%
  filter(!!sym(table_config$type_column) %in% codetype)

}
