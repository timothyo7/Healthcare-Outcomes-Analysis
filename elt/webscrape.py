# %%
# CMS Hospital Readmissions API Extract and Load
# This notebook extracts hospital readmission data from CMS Provider Data API
# and loads it to the PostgreSQL database raw schema

# Import required libraries
import requests
import pandas as pd
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time

# %%
# Load environment variables from .env file
load_dotenv()

# Database connection details from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

# %%
# Create a PostgreSQL connection string
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create a SQLAlchemy engine
engine = create_engine(connection_string)

# Test the connection
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Database connection successful!")
except Exception as e:
    print(f"Error connecting to database: {e}")

# %%
# Create raw schema if it doesn't exist
try:
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        print("Raw schema created or already exists")
except Exception as e:
    print(f"Error creating raw schema: {e}")

# %%
# CMS API Configuration
# Using the CMS Hospital Compare API

# Base URL for CMS Provider Data API
CMS_API_BASE_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u"

# API parameters to retrieve hospital readmission data
params = {
    "limit": 1000,  # Maximum number of records per request
    "offset": 0,    # Starting point for pagination
    "count": "true",  # Get total count of records
    "fields": "provider_id,hospital_name,address,city,state,zip_code,county_name,phone_number,measure_id,measure_name,score,national_rate,denominator,compared_to_national",
    "filter": {
        "measure_id": {
            "$in": [
                "READM-30-AMI", # Heart Attack Readmission
                "READM-30-HF",  # Heart Failure Readmission
                "READM-30-PN",  # Pneumonia Readmission
                "READM-30-COPD", # COPD Readmission
                "READM-30-HIP-KNEE", # Hip/Knee Readmission
                "READM-30-HOSP-WIDE" # Hospital-Wide Readmission
            ]
        }
    }
}

# %%
# Function to extract data from CMS API with pagination
def extract_cms_readmission_data():
    all_data = []
    params_copy = params.copy()
    total_records = None
    
    # Loop to handle pagination
    while True:
        try:
            # Convert params to JSON string for the request
            params_json = json.dumps(params_copy)
            
            # Make API request
            print(f"Fetching records from offset {params_copy['offset']}...")
            response = requests.get(
                CMS_API_BASE_URL,
                params={"query": params_json}
            )
            
            # Check if request was successful
            if response.status_code == 200:
                data = response.json()
                
                # Get total count of records if not already set
                if total_records is None and "count" in data:
                    total_records = data["count"]
                    print(f"Total records to fetch: {total_records}")
                
                # Append results to our data list
                if "results" in data and len(data["results"]) > 0:
                    all_data.extend(data["results"])
                    print(f"Fetched {len(data['results'])} records. Total records so far: {len(all_data)}")
                    
                    # Update offset for next page
                    params_copy["offset"] += params_copy["limit"]
                    
                    # Check if we've fetched all records
                    if len(all_data) >= total_records:
                        print("All records fetched!")
                        break
                        
                    # Add a pause to avoid hitting API rate limits
                    time.sleep(1)
                else:
                    print("No more records found or empty response.")
                    break
            else:
                print(f"Error fetching data: {response.status_code}")
                print(response.text)
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
# Display the first few rows of data
print(f"Fetched {len(cms_readmission_data)} rows of data")
print(f"Columns: {cms_readmission_data.columns.tolist()}")
cms_readmission_data.head()

# %%
# Examine data types and check for missing values
cms_readmission_data.info()

# %%
# Check for missing values
cms_readmission_data.isnull().sum()

# %%
# Basic data cleaning
def clean_readmission_data(df):
    # Create a copy to avoid SettingWithCopyWarning
    df_clean = df.copy()
    
    # Convert numeric fields from string to numeric
    numeric_columns = ['score', 'national_rate', 'denominator']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Clean up zip codes
    if 'zip_code' in df_clean.columns:
        df_clean['zip_code'] = df_clean['zip_code'].astype(str).str.replace(r'[^0-9]', '', regex=True)
        # Ensure 5-digit zip codes (pad with zeros if needed)
        df_clean['zip_code'] = df_clean['zip_code'].str[:5].str.zfill(5)
    
    return df_clean

# Apply cleaning
cms_readmission_data_clean = clean_readmission_data(cms_readmission_data)

# %%
# Load data to PostgreSQL raw schema
def load_to_postgres_raw(df, table_name, schema="raw"):
    full_table_name = f"{schema}.{table_name}"
    
    try:
        # Write DataFrame to PostgreSQL
        df.to_sql(
            name=table_name,
            schema=schema,
            con=engine,
            if_exists='replace',  # Replace if table already exists
            index=False,
            method='multi',
            chunksize=1000  # Load in chunks to avoid memory issues
        )
        print(f"Data successfully loaded to {full_table_name}")
        
        # Count rows in the table to confirm
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name}"))
            count = result.scalar()
            print(f"Row count in {full_table_name}: {count}")
            
    except Exception as e:
        print(f"Error loading data to {full_table_name}: {e}")

# %%
# Load data to raw.hospital_readmissions table
load_to_postgres_raw(cms_readmission_data_clean, "hospital_readmissions")

# %%
# Create a metadata table to track data extractions
extraction_metadata = pd.DataFrame([{
    'source': 'CMS Provider Data API',
    'dataset': 'hospital_readmissions',
    'rows_extracted': len(cms_readmission_data_clean),
    'extracted_at': datetime.now(),
    'extraction_status': 'success'
}])

# Load metadata to raw.extraction_metadata table
load_to_postgres_raw(extraction_metadata, "extraction_metadata")

# %%
# Close database connection
engine.dispose()
print("Database connection closed.")
