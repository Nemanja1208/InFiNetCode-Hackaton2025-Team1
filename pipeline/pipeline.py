import dlt
from .resources import wow_ah_resource, wow_item_resource, wow_item_class_indexes

# ----------- DLT source function to combine the resources -----------
@dlt.source
def wow_api_source(connected_realm_id: int = 1080):
    yield wow_ah_resource(connected_realm_id)
    yield wow_item_resource()
    yield wow_item_class_indexes()

# ---------- Function to create the DLT pipeline ----------
def create_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="wow_api_data",
        destination=dlt.destinations.duckdb("wow_api_data.duckdb"),
        dataset_name="raw"
    )
    return pipeline



# For testing purposes, you can run the pipeline directly
if __name__ == "__main__":
    pipeline = create_pipeline()
    load_info = pipeline.run(wow_api_source())
    # print(load_info)
