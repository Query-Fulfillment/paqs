#!/bin/bash

config_path="/root/dbconfig"

# Kerberos Authentication for Active Directory
# Extract the value of 'Trusted_Connection' from the JSON file using jq, convert to lowercase, and check if it's 'yes'
if [ "$(jq -r '.src_args.Trusted_Connection' "$config_path")" == 'yes' ]; then
    # If Trusted_Connection
    echo "Trusted_Connection=yes; Using Kerberos Authentication..."
    read -p "Provide database username including domain in uppercase (e.g. username@CHOP.EDU): " principal
    kinit $principal
fi

echo -e "\n\n"

Rscript query/execute_req.R

echo -e "\n\n\033[0;32mQury execution completed\033[0;0m"

bash tools/parse_log.sh