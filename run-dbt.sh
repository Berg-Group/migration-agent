#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

# Extract target_schema from migration_config.yml and export as TARGET_SCHEMA
if [ -f migration_config.yml ]; then
  TARGET_SCHEMA=$(grep '^target_schema:' migration_config.yml | sed 's/target_schema: *"\?\([^"]*\)"\?/\1/' | tr -d '"')
  export TARGET_SCHEMA
  echo "Loaded TARGET_SCHEMA: $TARGET_SCHEMA"
fi

# Change to DBT directory
cd DBT

# Run dbt with all passed arguments
dbt "$@"

# Return to parent directory
cd ..

