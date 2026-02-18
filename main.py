from extract_load_s3 import run_pipeline
from transform import run_transformation
from load_to_snowflake import run_snowflake_load
if __name__ == "__main__":
    # run_pipeline()
    # run_transformation()
    #  # Transform data
    # run_transformation()
    # Load into Snowflake
    run_snowflake_load()
