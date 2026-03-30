import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# --------------------------------------------------
# STEP 1: FETCH JSON FROM GITHUB RAW
# --------------------------------------------------

API_URL = "https://raw.githubusercontent.com/worldbank/open-api-specs/refs/heads/main/Data360%20Open_API.json"

# Call the API and get the JSON
response = requests.get(API_URL)
print("Status:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))

response.raise_for_status()  # Stop if not 200 OK

# Load JSON
json_data = response.json()

# --------------------------------------------------
# STEP 2: NORMALIZE JSON INTO DATAFRAME
# --------------------------------------------------

# Depending on the JSON structure, pick fields to flatten
# Here we will convert the top-level keys into a DataFrame
# (This JSON is nested; we’ll flatten a few high-level fields.)

# Convert to DataFrame
# If this JSON is deeply nested, adjust the logic accordingly
df = pd.json_normalize(json_data)

print(df.head())
print("Shape:", df.shape)

# Uppercase columns for Snowflake conventions
df.columns = [col.upper().replace(".", "_") for col in df.columns]

print(df.head())
print("Shape After Uppercase:", df.shape)

# --------------------------------------------------
# STEP 3: CONNECT TO SNOWFLAKE
# --------------------------------------------------

conn = snowflake.connector.connect(
    user="DATAMIKE1996",
    password="BubbleKiller1!",
    account="WHC15215.us-east-1",  # e.g., xy12345.us-east-1
    warehouse="python_API_Landing",
    database="Demo_db",
    schema="world_bank_info"
)

cursor = conn.cursor()

# --------------------------------------------------
# STEP 4: CREATE TARGET TABLE
# --------------------------------------------------

create_table_sql = """
CREATE OR REPLACE TABLE WORLD_BANK_API_SPEC (
    KEY_PATH VARCHAR,
    VALUE_STRING VARCHAR
)
"""
cursor.execute(create_table_sql)
print("Table created successfully")

# --------------------------------------------------
# STEP 5: PREPARE DATA FOR LOADING
# --------------------------------------------------

# For simplicity, flatten the DataFrame further by treating each column as key/value
# Build a new DataFrame with key/value pairs

records = []

for col in df.columns:
    records.append({"KEY_PATH": col, "VALUE_STRING": str(df[col].iloc[0])})

df_load = pd.DataFrame(records)

print(df_load.head())
print("Load Shape:", df_load.shape)

# --------------------------------------------------
# STEP 6: LOAD DATA INTO SNOWFLAKE
# --------------------------------------------------

success, nchunks, nrows, _ = write_pandas(
    conn,
    df_load,
    "WORLD_BANK_API_SPEC"
)

print(f"Loaded {nrows} rows into WORLD_BANK_API_SPEC")

# --------------------------------------------------
# STEP 7: VALIDATE LOAD
# --------------------------------------------------

cursor.execute("SELECT COUNT(*) FROM WORLD_BANK_API_SPEC")
print("Row count in Snowflake:", cursor.fetchone())

# Close connection
cursor.close()
conn.close()