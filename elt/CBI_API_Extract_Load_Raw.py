# %%
# Import necessary libraries
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import time

# %%
# Load environment variables from .env file
load_dotenv()

# %%
# Database connection details from .env
db_host = os.environ['DB_HOST']
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']
db_name = 'CBI'
db_port = '5432'

# %%
# Print connection parameters (excluding password)
print(f"Database host: {db_host}")
print(f"Database user: {db_user}")
print(f"Database schema: {db_schema}")
print(f"Database port: {db_port}")

# %%
# Build connection string and create database engine
conn_str = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(conn_str)

# %%
# Test database connection
try:
    connection = engine.connect()
    print("Successfully connected to the database!")
    connection.close()
except Exception as e:
    print(f"Error connecting to database: {e}")

# %%
# Define API endpoints
BASE_URL = "http://www.communitybenefitinsight.org/api"
HOSPITALS_ENDPOINT = f"{BASE_URL}/get_hospitals.php"
HOSPITAL_DATA_ENDPOINT = f"{BASE_URL}/get_hospital_data.php"

# %%
# Get hospitals data for states with high readmission rates
target_states = ['MA', 'NJ', 'FL', 'RI', 'CT']

all_hospitals = []
for state in target_states:
    print(f"Fetching hospitals for state: {state}")
    params = {'state': state}
    response = requests.get(HOSPITALS_ENDPOINT, params=params)
    hospitals = response.json()
    all_hospitals.extend(hospitals)
    print(f"Found {len(hospitals)} hospitals in {state}")

# %%
hospitals_df = pd.DataFrame(all_hospitals)
print(f"Total hospitals collected: {len(hospitals_df)}")
hospitals_df.head()

# %%
# Save hospitals to database
try:
    hospitals_df.to_sql('cbi_hospitals', engine, schema='raw', if_exists='replace', index=False)
    print(f"Saved {len(hospitals_df)} hospital records to database")
except Exception as e:
    print(f"Error saving hospitals to database: {e}")

# %%
# Get detailed hospital data for a sample of hospitals
sample_size = min(50, len(hospitals_df))
sample_hospitals = hospitals_df.sample(sample_size)

# %%
hospital_data = []
for index, hospital in sample_hospitals.iterrows():
    hospital_id = hospital['hospital_id']
    print(f"Fetching data for hospital ID: {hospital_id}")
    
    try:
        params = {'hospital_id': hospital_id}
        response = requests.get(HOSPITAL_DATA_ENDPOINT, params=params)
        
        # Check if response is successful
        if response.status_code == 200:
            try:
                data = response.json()
                hospital_data.extend(data)
                print(f"Retrieved {len(data)} years of data")
            except ValueError as e:
                print(f"Error parsing JSON for hospital ID {hospital_id}: {e}")
                print(f"Response content: {response.text[:300]}...")  # Print first 100 chars
        else:
            print(f"Error response for hospital ID {hospital_id}: Status code {response.status_code}")
            
    except Exception as e:
        print(f"Request error for hospital ID {hospital_id}: {e}")
    
    time.sleep(0.5)  

# %%
hospital_data_df = pd.DataFrame(hospital_data)
print(f"Total hospital data records: {len(hospital_data_df)}")
hospital_data_df.head()

# %%
# Save hospital data to database
try:
    if hospital_data:  # Check if we have any data to save
        hospital_data_df = pd.DataFrame(hospital_data)
        print(f"Total hospital data records: {len(hospital_data_df)}")
        hospital_data_df.to_sql('cbi_hospital_data', engine, schema='raw', if_exists='replace', index=False)
        print(f"Saved {len(hospital_data_df)} hospital data records to database")
    else:
        print("No hospital data to save")
except Exception as e:
    print(f"Error saving hospital data to database: {e}")
# %%
