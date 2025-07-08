"""
ETL script for running the DLT pipeline to collect data from the Blizzard API.

This script defines the `run_pipeline` function and can be run directly to execute
the pipeline using default settings.
"""

import dlt
from .resources import wow_api_source
import os

DB_PATH = os.path.abspath("wow_api_dbt/wow_api_data.duckdb")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


# Function for running the pipeline
def run_pipeline(sources=None, test_mode=False):
    pipeline = dlt.pipeline(
    pipeline_name = "wow_api_data",
    destination = dlt.destinations.duckdb(str(DB_PATH)),
    dataset_name = "raw",
    progress = "log"  # The progress reporting mode, can be "log", "console", or "none"
    )    
    if sources is not None:
        # If a specific source list is provided, we use it to run only those resources
        load_info = pipeline.run(wow_api_source(optional_source_list=sources, test_mode=test_mode))
    else:
        load_info = pipeline.run(wow_api_source(test_mode=test_mode))
    if load_info:    
        print(load_info)


# For testing
if __name__ == "__main__":
    run_pipeline()
