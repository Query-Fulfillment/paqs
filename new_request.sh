#!/bin/bash

# Set up new directory for new request for masking and creating reports
#
# Usage: /path/to/new_request.sh dir_name

target=${1:-new_request}
if [[ ! -z $(ls -d "$target"/* 2>/dev/null)  || \
      ! -z $(ls -d "$target"/.[a-zA-Z0-9]* 2>/dev/null | grep -v /.git\$ ) ]]
then
    echo >&2 "Target directory $target already has content."
    exit 1
fi
if [[ ! -d "$target" ]]; then mkdir -p "$target"; fi
target_path=$(cd "$target"; pwd)

src=$(dirname $0)
src=${src:-$PWD}
if [[ ! -d "$src" || ! -e "$src/new_request.sh" ]]
then
    echo >&2 "Unable to find paqs"
    exit 1
fi

cd "$src"

cp -r config_template package query results renv.lock tools Dockerfile workplan.qmd .gitignore .Rprofile "$target_path"

mkdir "$target_path"/renv

cp renv/activate.R renv/settings.json "$target_path"/renv

cd "$target_path"

# Rscript -e "usethis::create_project(path = getwd(),rstudio = TRUE)"

# rm -rf R