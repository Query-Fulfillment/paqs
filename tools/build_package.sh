#!/bin/bash

# Check if script is executed from the top-level directory
if [[ ! -d 'tools' ]]; then
    echo "Please execute from the top-level directory"
    exit 1
fi

req_name=$(basename "$PWD")

# grep -q "production" "query/execute_req.R"
# if [ $? -ne 0 ]; then
#   echo -e "\033[1;31m \nERROR: Session set to development. Please set the session to production mode in execute_req.R for packaging. See example # below\n\033[0;0m\n\ninitialize_session(\nsession_name = 'qf_query_session',\ndb_conn = "dbconfig",\nbase_directory ='./query',\nis_json = TRUE# ,\nresults_schema = NA,\nretain_intermediates = FALSE,\ndefault_file_output = TRUE,\nprep_dir = FALSE,\n\033[0;36mmode = "production")\033[0;0m"
#   exit 1
# fi
#
 grep -q "dbconfig" "query/execute_req.R"
 if [ $? -ne 0 ]; then
   echo -e "\033[1;31m \nERROR: Non-standard db_conn arguement for packaging. Please set the db_conn arguement to 'dbconfig' in execute_req.R for # packaging. See example below\n\033[0;0m\n\ninitialize_session(\nsession_name = 'qf_query_session',\n\033[0;36mdb_conn = 'dbconfig',\033[0# ;0m\nbase_directory ='./query',\nis_json = TRUE,\nresults_schema = NA,\nretain_intermediates = FALSE,\ndefault_file_output = TRUE,\nprep_dir = FALSE)"
   exit 1
 fi


# Create workplan directory
mkdir -p package

# Create a directory for query package

mkdir temp_dir_docker
mkdir temp_dir_docker/results
mkdir temp_dir_docker/query
mkdir temp_dir_docker/query/results
mkdir temp_dir_docker/tools
cp -r Dockerfile config_template temp_dir_docker
cp -r tools/banner tools/pcornet-style-sheet.css tools/handler.sh tools/parse_log.sh tools/PCORnet-logo-resize.png temp_dir_docker/tools
cp -r  query/script query/code_sets query/execute_req.R temp_dir_docker/query



cd temp_dir_docker
sed -i "s/Sys\.setenv('execution_mode' *= *'development')/Sys.setenv('execution_mode' = 'container')/" query/execute_req.R

# Create zip file with necessary contents
zip -r9X "${req_name}_docker.zip" *
mv "${req_name}_docker.zip" ../package
cd ..





mkdir temp_dir_native
mkdir temp_dir_native/query
mkdir temp_dir_native/query/results
mkdir temp_dir_native/tools
cp -r  *.Rproj config_template temp_dir_native
cp -r tools/banner tools/pcornet-style-sheet.css tools/PCORnet-logo-resize.png temp_dir_native/tools
cp -r  query/script query/code_sets query/execute_req.R temp_dir_native/query


# Render request_info.qmd to HTML
quarto render workplan.qmd --to html

# Move HTML file to workplan with modified name
mv workplan.html "package/${req_name}_workplan.html"


cd temp_dir_native
echo -e "execute_request <- function()  source('query/execute_req.R') " >> .Rprofile
sed -i "s/Sys\.setenv('execution_mode' *= *'development')/Sys.setenv('execution_mode' = 'nativeR')/" query/execute_req.R

# Create zip file with necessary contents
zip -r9X "${req_name}_nativeR.zip" * .Rprofile
mv "${req_name}_nativeR.zip" ../package
cd ..


# Make API calls to DockerHub to get latest tag
latest_tag=$(curl -s https://hub.docker.com/v2/namespaces/pcornetqf/repositories/qf-r-base/tags |\
 grep -o '"name":"[^"]*"' |\
 sed 's/"name":"\([^"]*\)"/\1/' |\
 grep -v latest |\
 sort |\
 tail -n 1)
# Get base image tag from Dockerfile
used_tag=$(grep '^FROM pcornetqf/qf-r-base' Dockerfile | awk -F ':' '{print $2}')
# Compare if latest
if [ "$latest_tag" != "$used_tag" ]; then
    echo -e "\033[0;31mWARNING: You are using $used_tag version of pcornetqf/qf-r-base, while the latest version on DockerHub is $latest_tag\033[0m"
else
    echo -e "\033[0;32mYou are using latest version of pcornetqf/qf-r-base: $used_tag\033[0m"
fi


# Remove qf-query-package directory
rm -rf temp_dir_docker
# Remove qf-query-package directory
rm -rf temp_dir_native
