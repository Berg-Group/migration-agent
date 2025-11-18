# Migration Agent

This repository contains two applications for data migration workflows:

1. **DBT** - Data transformation scripts using dbt (data build tool)
2. **QA Suite** - Quality assurance testing suite for validating migrated data

## Prerequisites

- **Python 3.6+** (for DBT)
- **Node.js 14+** (for QA Suite)
- **Redshift Database Access**
- **dbt CLI** (install via `pip install dbt-redshift`)

## Initial Setup

### 1. Environment Variables

Both applications share the same Redshift connection credentials. Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Then edit `.env` with your actual Redshift credentials:

```env
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your_database
REDSHIFT_DB=your_database
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
TARGET_SCHEMA=your_target_schema
```

**Important**: The `.env` file is gitignored and should never be committed to version control.

### 2. DBT Setup

#### Install Dependencies

```bash
cd DBT

# Create and activate Python virtual environment
python3 -m venv redshift_env
source redshift_env/bin/activate

# Install required packages
pip install dbt-redshift psycopg2-binary python-dotenv

# Install dbt packages (codegen, dbt_utils)
dbt deps
```

#### Configure DBT Project

If `dbt_project.yml` doesn't exist, create it from the template:

```bash
cp dbt_project.yml.example dbt_project.yml
```

Then edit `DBT/dbt_project.yml` to set your project-specific variables:

```yaml
vars:
  source_database: "your_source_db"
  clientName: "your_client"
  agency_id: "agency_id_value"
  master_id: "master_id_value"
  domain: "yourdomain.com"

models:
  transformations:
    +schema: "your_target_schema"

seeds:
  transformations:
    +schema: "your_target_schema"
```

#### Load Environment Variables

Before running dbt commands, you must load environment variables:

```bash
# Load .env variables into your shell
set -a && source ../.env && set +a
```

Or add it to your virtual environment activation:

```bash
# Add to redshift_env/bin/activate (at the end)
set -a && source /path/to/project/root/.env && set +a
```

#### Test Connection

```bash
python tests/test_redshift_connection.py
```

#### Run DBT

```bash
# Run all models
dbt run

# Run specific model folder
dbt run --select bullhorn.*

# Run tests
dbt test
```

### 3. QA Suite Setup

#### Install Dependencies

```bash
cd "QA Suite"

# Install Node.js dependencies
npm install
```

#### Run QA Tests

The QA Suite automatically loads environment variables from the root `.env` file.

```bash
# Run full test suite with verbose output
npm run qa

# Run with minimal output
npm run qa:quiet
```

#### Configure Table Testing

In your `.env` file:

- Leave `TABLE_PREFIXES` blank to test all tables
- Or specify specific tables: `TABLE_PREFIXES=companies,people,projects`
- Exclude tables: `EXCLUDED_TABLE_PREFIXES=temp,staging`

## Workflow

### Typical Migration Workflow

1. **Load source data** into Redshift staging schema
2. **Run DBT transformations** to clean and transform data
   ```bash
   cd DBT
   source redshift_env/bin/activate
   set -a && source ../.env && set +a
   dbt run --select your_client.*
   ```
3. **Run QA Suite** to validate transformed data
   ```bash
   cd "QA Suite"
   npm run qa
   ```
4. **Review QA logs** in `QA Suite/logs/` directory
5. **Fix issues** by updating DBT models and re-running transformations
6. **Re-test** until all validations pass

## Project Structure

```
Migration agent/
├── .env                    # Environment variables (gitignored)
├── .env.example           # Template for environment variables
├── README.md              # This file
├── DBT/                   # Data transformation scripts
│   ├── models/           # SQL transformation models
│   ├── macros/           # Custom dbt macros
│   ├── seeds/            # CSV seed files
│   ├── tests/            # dbt tests
│   ├── dbt_project.yml   # dbt configuration
│   ├── profiles.yml      # dbt connection config
│   └── redshift_env/     # Python virtual environment
└── QA Suite/             # Quality assurance testing
    ├── src/              # TypeScript source files
    ├── dist/             # Compiled JavaScript
    ├── logs/             # Test result logs
    └── package.json      # Node.js configuration
```

## Environment Variables Reference

| Variable | Used By | Description |
|----------|---------|-------------|
| `REDSHIFT_HOST` | Both | Redshift cluster endpoint |
| `REDSHIFT_PORT` | Both | Redshift port (usually 5439) |
| `REDSHIFT_DATABASE` | DBT | Database name for DBT |
| `REDSHIFT_DB` | QA Suite | Database name for QA Suite |
| `REDSHIFT_USER` | Both | Database username |
| `REDSHIFT_PASSWORD` | Both | Database password |
| `TARGET_SCHEMA` | QA Suite | Schema to validate |
| `SCHEMA_PREFIX` | DBT | Optional schema prefix |
| `TABLE_PREFIXES` | QA Suite | Tables to test (comma-separated) |
| `EXCLUDED_TABLE_PREFIXES` | QA Suite | Tables to exclude (comma-separated) |

## Troubleshooting

### DBT Issues

**Problem**: "Could not find profile"
- **Solution**: Ensure you've loaded environment variables with `set -a && source ../.env && set +a`

**Problem**: Connection timeout
- **Solution**: Check your Redshift security group allows connections from your IP
- Verify `REDSHIFT_HOST` and credentials in `.env`

**Problem**: "Package not found"
- **Solution**: Run `dbt deps` to install dbt packages

### QA Suite Issues

**Problem**: "Cannot find module"
- **Solution**: Run `npm install` in the QA Suite directory

**Problem**: TypeScript compilation errors
- **Solution**: Rebuild with `npm run build`

**Problem**: Connection refused
- **Solution**: Verify `.env` file exists in project root with correct credentials
- Check `REDSHIFT_DB` (not `REDSHIFT_DATABASE`) is set

## Additional Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [DBT Redshift Adapter](https://docs.getdbt.com/reference/warehouse-setups/redshift-setup)
- [QA Suite Video Tutorial](https://www.loom.com/share/aff3dd94b4f1488c94affa672b76f2fb)

## Support

For issues or questions, please refer to the individual README files in each subdirectory:
- `DBT/README.md`
- `QA Suite/README.md`

