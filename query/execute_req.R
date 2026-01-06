# Script Name: execute_req.R
# Description: This script serves as a wrapper around the query package, facilitating streamlined execution of queries.

# Author: Query Fulfillment Center for PCORnet®
# Date: 2024-11-04
# Version: 1.0

# Note: Ensure the working directory is set to this file's location before executing queries.
# If launched via the provided .Rproj file, the working directory will be set automatically.
# To manually set it, uncomment and modify the line below:

# setwd('path/to/location/of/this/file')

# Credentials Setup

# Active Directory Authentication:
# Step 1 : run `klist` in your teminal/cmd prompt to list the Kerberos principal and Kerberos tickets held in a credentials cache, or the keys held in a keytab file.
# Step 2 : If you have no cached tickets you will need to initialize one by running `kinit $principal` in the command line where your principal may be something like yourusername@SITE.EDU.
#     If you are unsure of your principal/domain, you can check the krb5.conf (Linux/Unix) or krb5.ini (Windows) file for additional information regarding specifications for capitalization, etc.

# Database Authentication:
#' Connection will be established at the runtime.
#' If you have opted to enter password interactively, a prompt will be generated upon execution to securely enter the password.
# Initialize a session

Sys.setenv('execution_mode' = 'development')
source("query/script/base/setup.R")
initialize_session(
	session_name = "",
	query_title = "",
	db_conn = "dbconfig",
	base_directory = './query',
	results_schema = NA,
	retain_intermediates = FALSE,
	default_file_output = TRUE,
	prep_dir = FALSE,

	#' Names of standard tables used in queries.
	#' @md
	#'
	#' [IMPORTANT]
	#' [Please edit only the right-hand side of each assignment.]
	#' [IMPORTANT All table names both on LHS and RHS must be lower-case;*]
	#' regardless of the case on your database. [cdm_case] parameter
	#' from the config file will determine which case should the
	#' SQL query be sent in.
	#'
	#' E.g.1 : If your table name is `DEMOGRAPHIC` the corresponding value should be
	#' `demographic = 'demographic'` and  `cdm_case` in the config file should be `upper`
	#'
	#' E.g.2  :If your table name is `pcor_demographic` the corresponding value should be
	#' `demographic = 'pcor_demographic'` and  `cdm_case` in the config file should be `lower`
	#'
	#'   #' E.g.3  :If your table name is `PCOR_DEMOGRAPHIC` the corresponding value should be
	#' `demographic = 'pcor_demographic'` and  `cdm_case` in the config file should be `upper`
	#'
	table_names = list(
		condition = 'condition',
		death = 'death',
		death_cause = 'death_cause',
		demographic = 'demographic',
		diagnosis = 'diagnosis',
		dispensing = 'dispensing',
		encounter = 'encounter',
		enrollment = 'enrollment',
		harvest = 'harvest',
		hash_token = 'hash_token',
		immunization = 'immunization',
		lab_result_cm = 'lab_result_cm',
		lds_address_history = 'lds_address_history',
		med_admin = 'med_admin',
		obs_clin = 'obs_clin',
		obs_gen = 'obs_gen',
		pcornet_trial = 'pcornet_trial',
		prescribing = 'prescribing',
		pro_cm = 'pro_cm',
		procedures = 'procedures',
		provider = 'provider',
		vital = 'vital'
	)
)


load_query(package = './query')


run()

clean_up()

