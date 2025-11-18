# DBT Project with Redshift Integration

Welcome to your new dbt project!

## Using the starter project

Try running the following commands:
- dbt run
- dbt test

## Loading Environment Variables for dbt Commands

**Important**: The `.env` file doesn't automatically get loaded when running dbt commands. You need to manually load the environment variables before running dbt.

### Method 1: Load variables in your current shell session

```bash
# From the transformations directory, run:
set -a && source .env && set +a
```

This will load all variables from the `.env` file into your current shell session. You'll need to do this each time you open a new terminal window or tab.

### Method 2: Add environment loading to your virtual environment

You can modify your virtual environment's activation script to automatically load variables:

```bash
# Add this line to the end of redshift_env/bin/activate
set -a && source /path/to/your/.env && set +a
```

After this modification, the variables will be loaded automatically whenever you activate the virtual environment.

## Redshift Connection Test

This project includes a test script to verify connectivity to your Redshift database using environment variables from a `.env` file.

### Prerequisites

1. Python 3.6+
2. Required packages:
   - `psycopg2` (or `psycopg2-binary`)
   - `python-dotenv`

### Setting Up Environment

We recommend using the dedicated virtual environment that has been set up for Redshift connection testing:

```bash
cd transformations
source redshift_env/bin/activate
```

If the `redshift_env` directory doesn't exist or you need to recreate it, you can do so with:

```bash
python3 -m venv redshift_env
source redshift_env/bin/activate
pip install psycopg2-binary python-dotenv
```

### Environment Variables

The script will look for a `.env` file in several locations, including the project root. If no `.env` file is found, the script will automatically create a sample one that you can edit.

Your `.env` file should contain these variables:

```
REDSHIFT_HOST=your-redshift-cluster.example.region.redshift.amazonaws.com
REDSHIFT_PORT=5439  # Optional, defaults to 5439
REDSHIFT_DATABASE=your_database_name
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
```

### Running the Test

With the virtual environment activated, run:

```bash
python tests/test_redshift_connection.py
```

### What the Test Does

1. Searches for and loads environment variables from a `.env` file
2. Creates a sample `.env` file if none is found
3. Checks that all required variables are present
4. Attempts to connect to the Redshift database
5. Runs a simple query to verify the connection
6. Displays the Redshift version and a list of available schemas

## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
