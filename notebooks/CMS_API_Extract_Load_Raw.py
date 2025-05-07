# %%
# CMS Hospital Readmissions API Extract and Load
# Data: https://data.cms.gov/provider-data/dataset/9n3s-kdb3#api

import requests
import pandas as pd
import json
import os
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
# CMS API configuration
# Hospital Readmissions Reduction Program dataset
CMS_API_BASE_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/9n3s-kdb3/0"

# %%
# Function to extract data with pagination
def extract_cms_readmission_data():
    all_data = []
    offset = 0
    limit = 500  # Records per request
    total_records = None
    
    while True:
        try:
            # Set parameters for pagination
            params = {
                "offset": offset,
                "limit": limit
            }
            
            print(f"Fetching records from offset {offset}...")
            response = requests.get(CMS_API_BASE_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                # Get total count on first request
                if total_records is None and "count" in data:
                    total_records = data.get("count")
                    print(f"Total records to fetch: {total_records}")
                
                # Check if we received results
                results = data.get("results", [])
                if results:
                    all_data.extend(results)
                    print(f"Fetched {len(results)} records. Total so far: {len(all_data)}")
                    
                    # Update offset for next page
                    offset += limit
                    
                    # Check if we've fetched all records
                    if total_records and len(all_data) >= total_records:
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
# Extract data from CMS API
cms_readmission_data = extract_cms_readmission_data()

# %%
# Display the first few rows and data info
print(f"Fetched {len(cms_readmission_data)} rows of data")
if not cms_readmission_data.empty:
    print(f"Columns: {cms_readmission_data.columns.tolist()}")
    display(cms_readmission_data.head())
    
    # Data types and missing values
    print("\nData types and info:")
    display(cms_readmission_data.info())
    print("\nMissing values per column:")
    display(cms_readmission_data.isnull().sum())
else:
    print("No data fetched. Please check the API connection.")

# %%
# Load data to PostgreSQL raw schema
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
# Load data to raw.hospital_readmissions table
if not cms_readmission_data.empty:
    load_to_postgres_raw(cms_readmission_data, "cms_hospital_readmissions")

    # Create a metadata table for tracking
    extraction_metadata = pd.DataFrame([{
        'source': 'CMS Hospital Readmissions Reduction Program API',
        'dataset': 'cms_hospital_readmissions',
        'rows_extracted': len(cms_readmission_data),
        'extracted_at': datetime.now(),
        'extraction_status': 'success'
    }])

    # Load metadata to tracking table
    load_to_postgres_raw(extraction_metadata, "extraction_metadata")
else:
    print("No data to load to database.")

# %%
# Close database connection
engine.dispose()
print("Database connection closed.")