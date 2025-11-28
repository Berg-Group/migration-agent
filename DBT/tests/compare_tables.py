#!/usr/bin/env python3
"""
Compare two person_identities_rcrm tables from different schemas.
Compares structure, row counts, and data field-by-field and row-by-row.
"""
import os
import sys
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
import json

def load_env():
    """Load environment variables from .env file."""
    env_paths = [
        Path(__file__).parents[2] / '.env',  # Parent directory (two levels up)
        Path(__file__).parents[1] / '.env',  # Tests directory
        Path(__file__).parent / '.env',      # Current directory
        Path.cwd() / '.env',                 # Current working directory
    ]
    
    for env_path in env_paths:
        if env_path.exists():
            print(f"Loading environment variables from: {env_path}")
            load_dotenv(dotenv_path=env_path)
            return True
    
    print("Error: No .env file found")
    return False

def get_connection():
    """Create and return a Redshift database connection."""
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT', '5439')
    database = os.getenv('REDSHIFT_DATABASE') or os.getenv('REDSHIFT_DB')
    user = os.getenv('REDSHIFT_USER')
    password = os.getenv('REDSHIFT_PASSWORD')
    
    missing = []
    if not host:
        missing.append('REDSHIFT_HOST')
    if not database:
        missing.append('REDSHIFT_DATABASE or REDSHIFT_DB')
    if not user:
        missing.append('REDSHIFT_USER')
    if not password:
        missing.append('REDSHIFT_PASSWORD')
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    conn_string = f"host={host} port={port} dbname={database} user={user} password={password}"
    return psycopg2.connect(conn_string)

def get_table_structure(conn, schema, table):
    """Get column information for a table."""
    cursor = conn.cursor()
    query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """
    cursor.execute(query, (schema, table))
    columns = cursor.fetchall()
    cursor.close()
    return columns

def get_table_data(conn, schema, table, order_by=None):
    """Get all data from a table."""
    cursor = conn.cursor()
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    query = f'SELECT * FROM "{schema}"."{table}" {order_clause};'
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return columns, rows

def get_row_count(conn, schema, table):
    """Get row count for a table."""
    cursor = conn.cursor()
    query = f'SELECT COUNT(*) FROM "{schema}"."{table}";'
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def compare_structures(struct1, struct2, schema1, schema2):
    """Compare table structures."""
    print("\n" + "="*80)
    print("STRUCTURE COMPARISON")
    print("="*80)
    
    cols1 = {col[0]: col for col in struct1}
    cols2 = {col[0]: col for col in struct2}
    
    all_cols = set(cols1.keys()) | set(cols2.keys())
    
    differences = []
    only_in_1 = []
    only_in_2 = []
    type_diffs = []
    
    for col in sorted(all_cols):
        if col in cols1 and col in cols2:
            col1 = cols1[col]
            col2 = cols2[col]
            if col1 != col2:
                type_diffs.append({
                    'column': col,
                    f'{schema1}': col1,
                    f'{schema2}': col2
                })
        elif col in cols1:
            only_in_1.append(col)
        else:
            only_in_2.append(col)
    
    print(f"\nColumns only in {schema1}: {len(only_in_1)}")
    if only_in_1:
        for col in only_in_1:
            print(f"  - {col}")
    
    print(f"\nColumns only in {schema2}: {len(only_in_2)}")
    if only_in_2:
        for col in only_in_2:
            print(f"  - {col}")
    
    print(f"\nColumns with type differences: {len(type_diffs)}")
    if type_diffs:
        for diff in type_diffs:
            print(f"\n  Column: {diff['column']}")
            print(f"    {schema1}: {diff[schema1]}")
            print(f"    {schema2}: {diff[schema2]}")
    
    common_cols = set(cols1.keys()) & set(cols2.keys())
    print(f"\nCommon columns: {len(common_cols)}")
    
    return {
        'only_in_1': only_in_1,
        'only_in_2': only_in_2,
        'type_diffs': type_diffs,
        'common_cols': sorted(common_cols)
    }

def normalize_value(val):
    """Normalize value for comparison (handle None, strings, etc.)."""
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip()
    return val

def compare_data(conn, schema1, table1, schema2, table2, common_cols, primary_key=None):
    """Compare data row by row and field by field."""
    print("\n" + "="*80)
    print("DATA COMPARISON")
    print("="*80)
    
    # Get row counts
    count1 = get_row_count(conn, schema1, table1)
    count2 = get_row_count(conn, schema2, table2)
    
    print(f"\nRow counts:")
    print(f"  {schema1}.{table1}: {count1:,} rows")
    print(f"  {schema2}.{table2}: {count2:,} rows")
    print(f"  Difference: {abs(count1 - count2):,} rows")
    
    # Get all data
    order_by = primary_key if primary_key and primary_key in common_cols else None
    cols1, rows1 = get_table_data(conn, schema1, table1, order_by)
    cols2, rows2 = get_table_data(conn, schema2, table2, order_by)
    
    # Create dictionaries for row lookup
    if primary_key and primary_key in common_cols:
        dict1 = {row[cols1.index(primary_key)]: row for row in rows1}
        dict2 = {row[cols2.index(primary_key)]: row for row in rows2}
        all_keys = set(dict1.keys()) | set(dict2.keys())
        
        only_in_1 = set(dict1.keys()) - set(dict2.keys())
        only_in_2 = set(dict2.keys()) - set(dict1.keys())
        common_keys = set(dict1.keys()) & set(dict2.keys())
        
        print(f"\nRows only in {schema1}: {len(only_in_1)}")
        if only_in_1 and len(only_in_1) <= 20:
            for key in sorted(only_in_1):
                print(f"  - {primary_key}: {key}")
        elif only_in_1:
            print(f"  (showing first 20 of {len(only_in_1)})")
            for key in sorted(list(only_in_1))[:20]:
                print(f"  - {primary_key}: {key}")
        
        print(f"\nRows only in {schema2}: {len(only_in_2)}")
        if only_in_2 and len(only_in_2) <= 20:
            for key in sorted(only_in_2):
                print(f"  - {primary_key}: {key}")
        elif only_in_2:
            print(f"  (showing first 20 of {len(only_in_2)})")
            for key in sorted(list(only_in_2))[:20]:
                print(f"  - {primary_key}: {key}")
        
        # Compare common rows field by field
        field_differences = defaultdict(list)
        rows_with_diffs = 0
        
        for key in common_keys:
            row1 = dict1[key]
            row2 = dict2[key]
            row_diffs = []
            
            for col in common_cols:
                idx1 = cols1.index(col)
                idx2 = cols2.index(col)
                val1 = normalize_value(row1[idx1])
                val2 = normalize_value(row2[idx2])
                
                if val1 != val2:
                    row_diffs.append({
                        'column': col,
                        f'{schema1}': val1,
                        f'{schema2}': val2
                    })
                    field_differences[col].append({
                        'key': key,
                        f'{schema1}': val1,
                        f'{schema2}': val2
                    })
            
            if row_diffs:
                rows_with_diffs += 1
        
        print(f"\nRows with field differences: {rows_with_diffs:,} out of {len(common_keys):,} common rows")
        
        # Show field-level differences
        print(f"\nField-level differences summary:")
        for col in sorted(common_cols):
            if col in field_differences:
                diff_count = len(field_differences[col])
                print(f"  {col}: {diff_count:,} differences ({diff_count/len(common_keys)*100:.2f}% of rows)")
                
                # Show sample differences
                if diff_count > 0 and diff_count <= 10:
                    print(f"    Sample differences:")
                    for diff in field_differences[col][:5]:
                        print(f"      {primary_key}={diff['key']}:")
                        print(f"        {schema1}: {diff[schema1]}")
                        print(f"        {schema2}: {diff[schema2]}")
                elif diff_count > 10:
                    print(f"    Sample differences (first 5):")
                    for diff in field_differences[col][:5]:
                        print(f"      {primary_key}={diff['key']}:")
                        print(f"        {schema1}: {diff[schema1]}")
                        print(f"        {schema2}: {diff[schema2]}")
        
        return {
            'count1': count1,
            'count2': count2,
            'only_in_1': list(only_in_1),
            'only_in_2': list(only_in_2),
            'rows_with_diffs': rows_with_diffs,
            'field_differences': dict(field_differences)
        }
    else:
        # No primary key, do positional comparison
        print("\nWarning: No primary key specified. Doing positional comparison.")
        min_rows = min(len(rows1), len(rows2))
        field_differences = defaultdict(list)
        rows_with_diffs = 0
        
        for i in range(min_rows):
            row1 = rows1[i]
            row2 = rows2[i]
            row_diffs = []
            
            for col in common_cols:
                idx1 = cols1.index(col)
                idx2 = cols2.index(col)
                val1 = normalize_value(row1[idx1])
                val2 = normalize_value(row2[idx2])
                
                if val1 != val2:
                    row_diffs.append({
                        'column': col,
                        f'{schema1}': val1,
                        f'{schema2}': val2
                    })
                    field_differences[col].append({
                        'row_index': i,
                        f'{schema1}': val1,
                        f'{schema2}': val2
                    })
            
            if row_diffs:
                rows_with_diffs += 1
        
        print(f"\nRows with field differences: {rows_with_diffs:,} out of {min_rows:,} compared rows")
        
        # Show field-level differences
        print(f"\nField-level differences summary:")
        for col in sorted(common_cols):
            if col in field_differences:
                diff_count = len(field_differences[col])
                print(f"  {col}: {diff_count:,} differences ({diff_count/min_rows*100:.2f}% of rows)")
        
        return {
            'count1': count1,
            'count2': count2,
            'rows_with_diffs': rows_with_diffs,
            'field_differences': dict(field_differences)
        }

def main():
    """Main comparison function."""
    import sys
    # Allow table name to be passed as argument, default to person_identities_rcrm
    if len(sys.argv) > 1:
        table = sys.argv[1]
    else:
        table = "person_identities_rcrm"
    
    schema1 = "yogi_lechley_migrated"  # PRODUCTION (reference)
    schema2 = "lechley_migrated_cursor_lechley_migrated_cursor"  # TEST/MIGRATION
    
    print("="*80)
    print(f"COMPARING TABLES: {schema1}.{table} (PRODUCTION) vs {schema2}.{table} (MIGRATION)")
    print("="*80)
    
    if not load_env():
        sys.exit(1)
    
    try:
        conn = get_connection()
        print(f"\nConnected to Redshift database")
        
        # Get table structures
        print(f"\nFetching table structures...")
        struct1 = get_table_structure(conn, schema1, table)
        struct2 = get_table_structure(conn, schema2, table)
        
        if not struct1:
            print(f"Error: Table {schema1}.{table} not found or empty")
            sys.exit(1)
        if not struct2:
            print(f"Error: Table {schema2}.{table} not found or empty")
            sys.exit(1)
        
        # Compare structures
        structure_diff = compare_structures(struct1, struct2, schema1, schema2)
        common_cols = structure_diff['common_cols']
        
        # Try to identify primary key (varies by table)
        possible_keys = ['id', 'person_identity_id', 'person_id', 'company_id']
        primary_key = None
        for key in possible_keys:
            if key in common_cols:
                primary_key = key
                break
        
        if primary_key:
            print(f"\nUsing '{primary_key}' as primary key for row matching")
        else:
            print(f"\nNo primary key found. Using positional comparison.")
        
        # Compare data
        data_diff = compare_data(conn, schema1, table, schema2, table, common_cols, primary_key)
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY (PRODUCTION vs MIGRATION)")
        print("="*80)
        print(f"Structure differences:")
        print(f"  Columns only in PRODUCTION ({schema1}): {len(structure_diff['only_in_1'])}")
        print(f"  Columns only in MIGRATION ({schema2}): {len(structure_diff['only_in_2'])}")
        print(f"  Columns with type differences: {len(structure_diff['type_diffs'])}")
        print(f"  Common columns: {len(common_cols)}")
        
        print(f"\nData differences:")
        print(f"  Row count difference: {abs(data_diff['count1'] - data_diff['count2']):,}")
        if 'only_in_1' in data_diff:
            print(f"  Rows missing in MIGRATION (only in PRODUCTION): {len(data_diff['only_in_1'])}")
            print(f"  Extra rows in MIGRATION (not in PRODUCTION): {len(data_diff['only_in_2'])}")
        print(f"  Rows with field differences: {data_diff['rows_with_diffs']:,}")
        print(f"  Fields with differences: {len(data_diff['field_differences'])}")
        
        conn.close()
        print("\nComparison complete!")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

