import json
import pandas as pd
import snowflake.connector
import boto3
import logging

# CONFIGURATION

BUCKET_NAME = "lithish-1-s3-bucket"
S3_KEY = "processed/posts_transformed.json"

SNOWFLAKE_CONFIG = {
    "user": "*********",
    "password": "********",
    "account": "LEPRVXT-XUC01698",
    "warehouse": "COMPUTE_WH",
    "database": "RETAIL_DB",
    "schema": "RAW_DATA"
}

logging.basicConfig(level=logging.INFO)


# READ FROM S3

def read_from_s3():
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=S3_KEY)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    logging.info("Processed data read from S3")
    return pd.DataFrame(data)


# LOAD TO SNOWFLAKE

def load_to_snowflake(df):
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )
    cs = ctx.cursor()
    try:
        for index, row in df.iterrows():
            cs.execute(
                "INSERT INTO posts_transformed (userId, id, title, body, title_length) VALUES (%s, %s, %s, %s, %s)",
                (row['userId'], row['id'], row['title'], row['body'], row['title_length'])
            )
        logging.info("Data loaded into Snowflake successfully")
    finally:
        cs.close()
        ctx.close()


# RUN PIPELINE

def run_snowflake_load():
    df = read_from_s3()
    load_to_snowflake(df)
