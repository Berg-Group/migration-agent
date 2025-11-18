# Migration Test Suite (TypeScript)

This migration test suite allows you to quickly validate data directly from RedShift before export.

## Setup

1. Create a `.env` file with the following variables:
   ```
   REDSHIFT_USER=your_username
   REDSHIFT_PASSWORD=your_password
   REDSHIFT_HOST=your_host
   REDSHIFT_PORT=5439
   REDSHIFT_DB=your_database
   TARGET_SCHEMA=your_schema
   TABLE_PREFIXES=
   ```

2. Install dependencies:
   ```
   npm install
   ```

## Running the Tests

Run the test suite with:
```
npm run qa
```

For less verbose output:
```
npm run qa:quiet
```

If you leave `.env` `TABLE_PREFIXES` blank, the script will validate all tables in the schema. If you'd like to focus on specific tables, you can provide a comma-separated list:
```
TABLE_PREFIXES=table1,table2,table3
```

## Video Tutorial

A video of how to create a test suite for a new table: https://www.loom.com/share/aff3dd94b4f1488c94affa672b76f2fb

## Available Validation Rules

| Rule in rules.yml | Validator Function | What it Checks |
|-------------------|-------------------|----------------|
| `iso_timestamp: [col, ...]` | matchesIso8601 | ISO-8601 format + non-null |
| `not_null: [col, ...]` | notNull | Non-null and non-blank |
| `required: [col, ...]` | mustExist | Column physically exists |
| `unique: [col, ...]` | unique | No duplicate values |
| `uuid_columns: [col, ...]` | matchesRegex + UUID regex | Valid UUID v4 |
| `allowed_values:` | acceptedValues | Value must be in list |
| `warn_null: col: fraction` | warnIfNullFraction | % NULL must stay below N |
| `constant: col: value` | columnIsConstant | Every row = given value |
| `constant_across_table: [col]` | columnIsConstant | One shared value for table |
| `no_html_columns: [col, ...]` | noHtml | Column must not contain HTML tags |
| `date_ymd_columns: [col, ...]` | matchesDateYmd | Strict YYYY-MM-DD format |
| `currency: [col, ...]` | currency | Fails if not 3 letters, warns if not valid currency |
| `numeric: [col, ...]` | numeric | Validates that values are numeric (integers, decimals, floats) |
| `trim: [col, ...]` | trim | No leading or trailing whitespace |
| `nice_to_have: [col, ...]` | | Logs WARNING if missing (does not fail the run) | 