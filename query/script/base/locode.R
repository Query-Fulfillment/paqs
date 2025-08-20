dx_any_icd10  <- read_csv(file = 'query/code_sets/dx_any_icd_10.csv')


if(all(pull(codesets$dx_any_icd_10, code) == "*")) {

codetype <- codeset %>% pull(codetype)
 
cdm_tbl(table_name) %>%
  filter(!!sym(table_config$type_column) %in% codetype)

}