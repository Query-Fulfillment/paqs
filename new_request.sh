#!/bin/bash
set -euo pipefail

# ==============================================================================
# new_request.sh — Set up a new request directory for masking and reports
# ==============================================================================
# Usage:
#   ./new_request.sh <new_dir_name>
#
# Example:
#   ./new_request.sh cohort_2025_10_30
# ==============================================================================

# --- Colors for pretty output ---
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
RED='\033[1;31m'
NC='\033[0m' # No Color

# --- Help function ---
usage() {
  cat <<EOF
Usage: $(basename "$0") <new_dir_name>

Creates a new request directory for masking and report generation.

Options:
  -h, --help     Show this help message and exit

Example:
  $(basename "$0") my_new_request
EOF
  exit 0
}

# --- Argument parsing ---
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
fi

target=${1:-}
if [[ -z "$target" ]]; then
  echo -e "${RED}Error:${NC} No directory name provided."
  echo "Run with --help for usage information."
  exit 1
fi

# --- Check if target directory exists and is empty ---
if [[ -d "$target" && ( ! -z "$(ls -A "$target" 2>/dev/null)" ) ]]; then
  echo -e "${RED}Error:${NC} Target directory '$target' already exists and is not empty."
  exit 1
fi

# --- Create directory if needed ---
echo -e "${BLUE}▶ Setting up new request directory: ${YELLOW}$target${NC}"
mkdir -p "$target"
target_path=$(cd "$target" && pwd)

# --- Locate source directory (this script’s location) ---
src_dir=$(cd "$(dirname "$0")" && pwd)
if [[ ! -f "$src_dir/new_request.sh" ]]; then
  echo -e "${RED}Error:${NC} Unable to find source files in '$src_dir'."
  exit 1
fi

# --- Verify required files exist ---
required_items=(config_template package query results tools Dockerfile workplan.qmd .gitignore paqs.Rproj)
for item in "${required_items[@]}"; do
  if [[ ! -e "$src_dir/$item" ]]; then
    echo -e "${RED}Error:${NC} Missing required file or directory: $item"
    exit 1
  fi
done

# --- Copy main project components ---
echo -e "${BLUE}▶ Copying core project files...${NC}"
cp -r "$src_dir"/{config_template,package,query,results,tools,Dockerfile,workplan.qmd,.gitignore} "$target_path"


# --- Copy and rename R project file ---
echo -e "${BLUE}▶ Creating R project file...${NC}"
cp "$src_dir/paqs.Rproj" "$target_path/${target}.Rproj"

# --- Final summary ---
echo -e "${GREEN}✅ Successfully created new request directory!${NC}"
echo -e "Location: ${YELLOW}$target_path${NC}"
echo -e "Project file: ${YELLOW}${target}.Rproj${NC}"
echo -e "\nNext steps:"
echo -e "  1. cd ${YELLOW}$target${NC}"
echo -e "  2. Open ${YELLOW}${target}.Rproj${NC} in RStudio"
echo -e "  3. Start Query Development"
echo
