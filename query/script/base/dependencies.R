if(Sys.getenv('execution_mode') %in% c('development','container')) {
# call libraries
packages <- c(
  "argos",
  "DBI",
  "dbplyr",
  "RPostgres",
  "odbc",
  "bigrquery",
  "cli",
  "pak",
  "duckdb",
  "quarto",
  "tidyverse"
)

for (pak in packages) {
  suppressWarnings(suppressPackageStartupMessages(require(
    pak,
    character.only = TRUE
  )))
}
} else if (Sys.getenv('execution_mode') %in% c('nativeR')) {
  pkgLoad()
} else {
  cli_abort("Incorrect execution mode. Permitted values are `development`, `container` and `nativeR`")
}


suppressMessages(conflicted::conflict_prefer_all("dplyr"))

patch_argos()
patch_srcr()
patch_squba()

library("srcr")
library("squba.gen")

unloadNamespace("expectedvariablespresent")
patch_squba()
library("expectedvariablespresent")

unloadNamespace("sensitivityselectioncriteria")
patch_squba()
library("sensitivityselectioncriteria")


unloadNamespace("conceptsetdistribution")
patch_squba()
library("conceptsetdistribution")

unloadNamespace("clinicalevents.specialties")
patch_squba()
library("clinicalevents.specialties")

unloadNamespace("patienteventsequencing")
patch_squba()
library("patienteventsequencing")

unloadNamespace("patientrecordconsistency")
patch_squba()
library("patientrecordconsistency")

unloadNamespace('clinicalevents.specialties')
patch_squba()
library('clinicalevents.specialties')
