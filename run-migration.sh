#!/bin/bash

# Temporary script to run DBT migration for lechley_associates

cd "/Users/jordanshlosberg/Library/CloudStorage/OneDrive-AtlasTechnology/Scripts/Migration agent/DBT"

# Load environment variables
set -a
source ../.env
set +a

# Set TARGET_SCHEMA
export TARGET_SCHEMA="lechley_migrated_cursor"

# Run DBT
./redshift_env/bin/dbt run --select clients.lechley_associates.* --profiles-dir . --project-dir .

