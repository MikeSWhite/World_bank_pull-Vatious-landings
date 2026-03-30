import requests
import json
import boto3
from datetime import datetime

# --------------------------
# Configuration
# --------------------------
API_URL = "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&per_page=5000"

BUCKET_NAME = "healthcare-data-pipeline-mw1"
S3_PREFIX = "python_test"

AWS_ACCESS_KEY = 'YOUR_ACCESS_KEY_HERE'
AWS_SECRET_KEY = "YOUR_SECRET_KEY_HERE"
REGION = "us-east-1"

# --------------------------
# Step 1: Pull data from API
# --------------------------
def fetch_api_data(url):
    response = requests.get(url)
    response.raise_for_status()  # stop if API call fails
    data = response.json()
    return data

# --------------------------
# Step 2: Save data locally
# --------------------------
def save_json_locally(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Saved API data to {filename}")

# --------------------------
# Step 3: Upload to S3
# --------------------------
def upload_to_s3(local_file, bucket, s3_prefix):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION
    )
    # create a timestamped file name in S3
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{s3_prefix}/worldbank_data_{timestamp}.json"

    s3_client.upload_file(local_file, bucket, s3_key)
    print(f"Uploaded {local_file} to s3://{bucket}/{s3_key}")

# --------------------------
# Main pipeline
# --------------------------
def main():
    print("Starting pipeline...")
    data = fetch_api_data(API_URL)
    local_file = "worldbank_data.json"
    save_json_locally(data, local_file)
    upload_to_s3(local_file, BUCKET_NAME, S3_PREFIX)
    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()