import dlt
import requests
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Ensure you have the required environment variables set
CLIENT_ID = os.getenv("BLIZZARD_CLIENT_ID")
CLIENT_SECRET = os.getenv("BLIZZARD_CLIENT_SECRET")

# Function to get access token from Blizzard API
def get_access_token():
    response = requests.post(
        "https://oauth.battle.net/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
    )
    response.raise_for_status()
    return response.json()["access_token"]

# Function to fetch auction house data from the World of Warcraft API
@dlt.resource(write_disposition="replace", name="wow_auctions")
def wow_ah_resource(connected_realm_id: int = 1080):
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/connected-realm/{connected_realm_id}/auctions"
    params = {
        "namespace": "dynamic-eu",
        "locale": "en_EU"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    # Yield the relevant data
    for auction in data.get("auctions", []):
        yield auction

# ---------- Function to fetch item data from the World of Warcraft API ----------
@dlt.resource(write_disposition="replace", name="wow_items_old")
def wow_item_resource_old():
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/search/item"
    params = {
        "namespace": "static-eu",
        "orderby": "id",
        "_page": 1,
    }

    # Fetch the results using the helper function
    data = _get_results(url, headers, params)

    # Yield the relevant data
    for item in data.get("results", []):
        yield item["data"]

# DLT source function to combine the resources
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


# ---------- Function to fetch item class indexes from the World of Warcraft API ----------
@dlt.resource(write_disposition="replace", name="wow_item_class_indexes")
def wow_item_class_indexes():
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/item-class/index"
    params = {
        "namespace": "static-eu",
        "locale": "en_US",
    }

        # Fetch the results using the helper function
    data = _get_results(url, headers, params)

    results = data.get("item_classes", [])

    # Yield the relevant data
    for item_class in results:
        yield item_class


# ---------- Function to fetch item data with pagination ----------
@dlt.resource(write_disposition="replace", name="wow_items")
def wow_item_resource():
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/search/item"
    params = {
        "namespace": "static-eu",
        "orderby": "id",
        "_page": 1,
        "locale": "en_US",
    }

    page = params.get("_page", 1)

    # Loop through pages of results
    while True:
        page_params = dict(params, _page=page) # Add the current page to the params

        # Get the data using the helper function
        data = _get_results(url, headers, page_params)

        results = data.get("results", []) # Extract the list of results

        # If there are no more results, stop the loop (end of pagination)
        if not results:
            print(f"⚠️ No more results found. Stopping pagination.")
            break

        # Yield the relevant data
        for item in results:
            yield item["data"]
        
        # If fewer results than the pageSize (100) is returned, break the loop
        if page == 11:
            print(f"⚠️ 1000 results fetched - limit reached for this run.")
            break
        
        # Print the number of results fetched in this batch
        print(f"Fetched {len(results)} results...")

        # Update the page variable to fetch the next page of results
        page += 1

# --- Helper function for making a GET request to the API ---
def _get_results(url, headers, params):
    response = requests.get(url, headers=headers, params=params) # Send GET request with parameters
    response.raise_for_status() # Raise an error for failed requests (non-2xx HTTP status)
    return json.loads(response.content.decode("utf8")) # Decode the JSON response into a dictionary


# For testing purposes, you can run the pipeline directly
if __name__ == "__main__":
    pipeline = create_pipeline()
    load_info = pipeline.run(wow_api_source())
    print(load_info)
