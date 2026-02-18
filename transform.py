import boto3
import json
import logging

BUCKET_NAME = "lithish-1-s3-bucket"
SOURCE_KEY = "raw/posts/posts_20260218_115430.json"
TARGET_KEY = "processed/posts_transformed.json"

logging.basicConfig(level=logging.INFO)

def read_from_s3():
    s3 = boto3.client("s3")

    response = s3.get_object(Bucket=BUCKET_NAME, Key=SOURCE_KEY)
    data = json.loads(response["Body"].read().decode("utf-8"))

    logging.info("Data read successfully from S3")
    return data

def transform_data(data):
    transformed = []

    for record in data:
        record["title"] = record["title"].upper()          # Transformation 1
        record["title_length"] = len(record["title"])      # Transformation 2
        transformed.append(record)

    logging.info("Data transformed successfully")
    return transformed

def write_to_s3(data):
    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=TARGET_KEY,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    logging.info("Transformed data uploaded to S3")

def run_transformation():
    raw_data = read_from_s3()
    transformed_data = transform_data(raw_data)
    write_to_s3(transformed_data)
