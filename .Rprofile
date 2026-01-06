# source("renv/activate.R")

packages <- c(
	'argos',
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
	'bigrquery',
	'quarto',
	"glue"
)

for (pak in packages) {
	suppressWarnings(suppressPackageStartupMessages(require(
		pak,
		character.only = TRUE
	)))
}

rm(packages)
rm(pak)


source("query/script/base/setup.R")
