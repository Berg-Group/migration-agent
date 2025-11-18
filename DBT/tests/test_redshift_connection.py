#!/usr/bin/env python3
import os
import sys
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

def test_redshift_connection():
    """Test connection to Redshift database using environment variables."""
    # Try loading environment variables from multiple locations
    env_paths = [
        Path(__file__).parents[2] / '.env',  # Parent directory (two levels up)
        Path(__file__).parents[1] / '.env',  # Tests directory
        Path(__file__).parent / '.env',      # Current directory
        Path.cwd() / '.env',                 # Current working directory
    ]
    
    # Try each path until we find one
    env_loaded = False
    for env_path in env_paths:
        if env_path.exists():
            print(f"Loading environment variables from: {env_path}")
            load_dotenv(dotenv_path=env_path)
            env_loaded = True
            break
    
    if not env_loaded:
        print("No .env file found in any of the following locations:")
        for path in env_paths:
            print(f"  - {path}")
        print("\nCreating a .env file in the tests directory...")
        create_sample_env_file(env_paths[1])
        return False
    
    # Redshift connection parameters from environment variables
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT', '5439')  # Default Redshift port is 5439
    database = os.getenv('REDSHIFT_DATABASE')
    user = os.getenv('REDSHIFT_USER')
    password = os.getenv('REDSHIFT_PASSWORD')
    
    # Check if all required environment variables are set
    required_vars = ['REDSHIFT_HOST', 'REDSHIFT_DATABASE', 'REDSHIFT_USER', 'REDSHIFT_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        create_sample_env_file(env_paths[1])
        return False
    
    # Try to connect to Redshift
    try:
        conn_string = f"host={host} port={port} dbname={database} user={user} password={password}"
        print(f"Connecting to Redshift database: {host}:{port}/{database} as {user}")
        
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"Successfully connected to Redshift!")
        print(f"Redshift version: {version[0]}")
        
        # Get list of schemas
        cursor.execute("SELECT DISTINCT table_schema FROM information_schema.tables LIMIT 10;")
        schemas = cursor.fetchall()
        print("\nAvailable schemas (first 10):")
        for schema in schemas:
            print(f"- {schema[0]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        return False

def create_sample_env_file(file_path):
    """Create a sample .env file with Redshift connection variables."""
    print(f"Creating a sample .env file at: {file_path}")
    with open(file_path, 'w') as f:
        f.write("""# Redshift Connection Settings
REDSHIFT_HOST=your-redshift-cluster.example.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your_database_name
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password

# Update with your actual Redshift credentials before running the test
""")
    print(f"Sample .env file created. Please update it with your actual Redshift credentials.")
    print(f"Then run the test again: python3 tests/test_redshift_connection.py")

if __name__ == "__main__":
    success = test_redshift_connection()
    sys.exit(0 if success else 1) 