# Client Investigation

This folder contains client-specific investigation scripts, test scripts, and data comparison tools.

## Structure

Each client has their own folder:
```
client_investigation/
  └── <client_name>/
      ├── compare_tables.py
      ├── test_data_quality.py
      └── ... (other investigation scripts)
```

## Purpose

- **Organized by client**: All investigation work for a specific client is contained in their folder
- **Keeps database clean**: Prevents orphaned scripts from cluttering the main codebase
- **Easy cleanup**: Client-specific investigation work can be easily identified and removed after migration

## Usage

When creating investigation scripts:
1. Read `client_name` from `migration_config.yml`
2. Create folder: `client_investigation/<client_name>/` if it doesn't exist
3. Place all test scripts, comparison scripts, and investigation tools in that folder

## Git Ignore

All client investigation folders are ignored by git (see `.gitignore` in this directory).

