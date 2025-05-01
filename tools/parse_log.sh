#!/bin/bash

RED='\033[1;31m'
ORANGE='\033[0;33m'
GREEN='\033[0;32m'
NC='\033[0m'
CYAN='\033[0;36m'
echo -e "Query execution completed..."

print_warnings() {

    echo -n "Parsing query log for warnings"
    for ((i=0; i<3; i++)); do 
    printf "."
    sleep 1
    done
}

parsing_errors() {

    echo -n "Parsing query log for errors"

    for ((i=0; i<3; i++)); do 
    printf "."
    sleep 1
    done

}

# Construct the full path to the log file
   log_file=( $(find . -type f -name "*.log"))

# Find all log files in the directory and subdirectories
while IFS= read -r -d '' log_file; do



    print_warnings
    warning_string="WARNING"
    warning_lines=$(grep -i "$warning_string" "$log_file")



    if [ -n "$warning_lines" ]; then
        echo -e "\n\n${ORANGE}Query returned with following warnings in the log file: $log_file\n$warning_lines${NC}\n\n"
    else
        echo -e "\n\n${GREEN}No warnings found in the log file: $log_file.${NC}\n\n"
    fi

    parsing_errors
    error_string="ERROR"
    error_lines=$(grep -i "$error_string" "$log_file")

    # Print error lines
    if [ -n "$error_lines" ]; then
        echo -e "${RED}\n\nQuery returned with following errors in the log file: $log_file\n\n$error_lines \n\n\n${NC}"
        echo -e "${RED}Query return with errors. Please consult PCORnet QF center at qf@pcornet.org along with attaching the log file from results folder${NC}"
    else
        echo -e "\n\n${GREEN}No errors found in the log file: $log_file. Please refer to Output Files section of the workplan for next steps.${NC}\n\n"
    fi

done < <(find . -type f -name "*.log" -print0)

 echo -e "\n\nThe container that ran this query will be removed my default. \nHowever, the query image still persists on the host machine. \n\nIf you would like to remove the query image as well, execute the following line replacing {docker-image-name} with name specific to this query.\n\n"
 echo -e "${CYAN}docker image rm {docker-image-name}${NC}"