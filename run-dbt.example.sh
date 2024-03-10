#!/bin/sh
# Make a copy of this file and rename it to run-dbt.sh

# Variables
project_dir="/Path/to/dbt/project/dir"
virtual_env_dir="/Path/to/dbt/environment/"

# Activate the virtual environment
"$virtual_env_dir"/bin/dbt run --project-dir "$project_dir" --profiles-dir "$project_dir"/profiles
