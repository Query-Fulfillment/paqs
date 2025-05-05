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

if(Sys.getenv('native_execution')) {
  renv::restore(prompt = FALSE)
}

initialize_session(session_name = "qf_query_session",
                   db_conn = "bigQuery",
									 base_directory ='./query',
                   is_json = TRUE,
                   results_schema = NA,
                   retain_intermediates = FALSE,
                   default_file_output = TRUE,
                   prep_dir = FALSE)


load_query(package = './query')


run()
