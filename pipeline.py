import dlt
from resources import load_ah_data

def create_pipeline():
    return dlt.pipeline(

        pipeline_name="wow_ah_pipeline",
        dataset_name="raw_auctions",
        destination=dlt.destinations.duckdb(
            database="wow.duckdb",
            schema="raw",
            table_name="raw_auctions"
        ),
    )

def run_pipeline():
    pipeline = create_pipeline()
    pipeline.run(load_ah_data())
    print("Pipeline run completed successfully.")

if __name__ == "__main__":
    run_pipeline()
    print("Pipeline executed successfully.")