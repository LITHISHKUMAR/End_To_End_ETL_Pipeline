import requests
import json
import boto3
from datetime import datetime
import logging

# CONFIGURATION
API_URL = "https://jsonplaceholder.typicode.com/posts"
BUCKET_NAME = "lithish-1-s3-bucket"
S3_FOLDER = "raw/posts"

logging.basicConfig(level=logging.INFO)

def extract_data():
    response = requests.get(API_URL)
    response.raise_for_status()
    logging.info("API data extracted successfully")
    return response.json()

def load_to_s3(data):
    s3_client = boto3.client("s3")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_key = f"{S3_FOLDER}/posts_{timestamp}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    logging.info(f"File uploaded successfully to S3: {file_key}")

def run_pipeline():
    data = extract_data()
    load_to_s3(data)
