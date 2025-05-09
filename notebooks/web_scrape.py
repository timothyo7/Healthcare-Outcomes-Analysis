# %%
# NY Hospital Safety & Infection Rates Web Scraping Script
# Data Source: New York State Department of Health

import requests
import pandas as pd
import os
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time

# %%
# Load environment variables
load_dotenv()

# Database connection details
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# %%
# Create PostgreSQL connection
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Test connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Database connection successful!")
except Exception as e:
    print(f"Error connecting to database: {e}")

# %%
# Create raw schema if needed
try:
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        print("Raw schema created or already exists")
except Exception as e:
    print(f"Error creating raw schema: {e}")

# %%
# NY State Hospital-Acquired Infections API Configuration
# Using Socrata Open Data API (SODA)
NY_HAI_BASE_URL = "https://health.data.ny.gov/resource/utrt-zdsi.json"

# %%
# Function to process nested dictionary fields to make them compatible with PostgreSQL
def process_nested_dicts(df):
    """Convert any dictionary or list columns to JSON strings"""
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            print(f"Converting column {col} from dict/list to JSON string")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

# %%
# Function to load data to PostgreSQL
def load_to_postgres_raw(df, table_name, schema="raw"):
    if df.empty:
        print(f"No data to load to {schema}.{table_name}")
        return
        
    full_table_name = f"{schema}.{table_name}"
    
    try:
        # Write DataFrame to PostgreSQL
        df.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists='replace',  # Replace if table exists
            index=False,
            method='multi',
            chunksize=1000  # Load in chunks
        )
        print(f"Data successfully loaded to {full_table_name}")
        
        # Count rows to confirm
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name}"))
            count = result.scalar()
            print(f"Row count in {full_table_name}: {count}")
            
    except Exception as e:
        print(f"Error loading data to {full_table_name}: {e}")

# %%
# Function to extract data with pagination
def extract_ny_hospital_infections_data():
    all_data = []
    offset = 0
    limit = 1000  # Records per request
    
    while True:
        try:
            # Set parameters for pagination
            params = {
                "$offset": offset,
                "$limit": limit
            }
            
            print(f"Fetching records from offset {offset}...")
            response = requests.get(NY_HAI_BASE_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if we received results
                if data:
                    all_data.extend(data)
                    print(f"Fetched {len(data)} records. Total so far: {len(all_data)}")
                    
                    # Update offset for next page
                    offset += limit
                    
                    # Check if we've fetched all records
                    if len(data) < limit:
                        print("All records fetched!")
                        break
                        
                    # Add a pause to avoid rate limits
                    time.sleep(0.5)
                else:
                    print("No more results found.")
                    break
            else:
                print(f"Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"Error during API request: {e}")
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Add extraction timestamp
    df['extracted_at'] = datetime.now()
    
    return df

# %%
# Function to extract hospital profiles data
def extract_ny_hospital_profiles():
    NY_HOSPITAL_PROFILES_URL = "https://health.data.ny.gov/resource/vn5v-hh5r.json"
    all_data = []
    offset = 0
    limit = 1000  # Records per request
    
    while True:
        try:
            # Set parameters for pagination
            params = {
                "$offset": offset,
                "$limit": limit
            }
            
            print(f"Fetching hospital profiles from offset {offset}...")
            response = requests.get(NY_HOSPITAL_PROFILES_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if we received results
                if data:
                    all_data.extend(data)
                    print(f"Fetched {len(data)} profiles. Total so far: {len(all_data)}")
                    
                    # Update offset for next page
                    offset += limit
                    
                    # Check if we've fetched all records
                    if len(data) < limit:
                        print("All hospital profiles fetched!")
                        break
                        
                    # Add a pause to avoid rate limits
                    time.sleep(0.5)
                else:
                    print("No more hospital profiles found.")
                    break
            else:
                print(f"Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"Error during hospital profiles API request: {e}")
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Add extraction timestamp
    df['extracted_at'] = datetime.now()
    
    return df

# %%
# Extract data from NY State Hospital-Acquired Infections API
ny_hospital_infections_data = extract_ny_hospital_infections_data()

# %%
# Display the first few rows and data info
print(f"Fetched {len(ny_hospital_infections_data)} rows of data")
if not ny_hospital_infections_data.empty:
    print(f"Columns: {ny_hospital_infections_data.columns.tolist()}")
    print(ny_hospital_infections_data.head())
    
    # Data types and missing values
    print("\nData types and info:")
    print(ny_hospital_infections_data.info())
    print("\nMissing values per column:")
    print(ny_hospital_infections_data.isnull().sum())
else:
    print("No data fetched. Please check the API connection.")

# %%
# Process nested dictionary fields and load infections data to database
if not ny_hospital_infections_data.empty:
    print("Processing nested dictionaries in infections data...")
    ny_hospital_infections_data = process_nested_dicts(ny_hospital_infections_data)
    
    # Load data to raw.ny_hospital_infections table
    load_to_postgres_raw(ny_hospital_infections_data, "ny_hospital_infections")

    # Create a metadata table for tracking
    extraction_metadata = pd.DataFrame([{
        'source': 'NY State Department of Health - Hospital-Acquired Infections',
        'dataset': 'ny_hospital_infections',
        'rows_extracted': len(ny_hospital_infections_data),
        'extracted_at': datetime.now(),
        'extraction_status': 'success'
    }])

    # Load metadata to tracking table
    load_to_postgres_raw(extraction_metadata, "extraction_metadata")
else:
    print("No data to load to database.")

# %%
# Extract hospital profiles data
ny_hospital_profiles = extract_ny_hospital_profiles()

# %%
# Display the hospital profiles info and load to database
if not ny_hospital_profiles.empty:
    print(f"Fetched {len(ny_hospital_profiles)} hospital profiles")
    print(f"Hospital profile columns: {ny_hospital_profiles.columns.tolist()}")
    print(ny_hospital_profiles.head())
    
    # Process nested dictionaries
    print("Processing nested dictionaries in hospital profiles...")
    ny_hospital_profiles = process_nested_dicts(ny_hospital_profiles)
    
    # Load to database
    load_to_postgres_raw(ny_hospital_profiles, "ny_hospital_profiles")
else:
    print("No hospital profiles fetched.")

# %%
# Close database connection
engine.dispose()
print("Database connection closed.")