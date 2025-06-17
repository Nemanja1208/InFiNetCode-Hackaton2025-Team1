# Import libraries
import dlt
import requests
import json

from .utils import get_access_token

# ---------- DLT RESOURCES ----------
# --- Helper function for making a GET request to the API ---
def _get_results(url, headers, params):
    response = requests.get(url, headers=headers, params=params) # Send GET request with parameters
    response.raise_for_status() # Raise an error for failed requests (non-2xx HTTP status)
    print(f"Status code: {response.status_code}\n")
    print(f"Headers: {response.headers}\n")
    print(f"Response: {response.text}\n")
    return json.loads(response.content.decode("utf8")) # Decode the JSON response into a dictionary


# ---------- Function to fetch auction house data from the World of Warcraft API ----------
@dlt.resource(write_disposition="replace", name="wow_auctions")
def wow_ah_resource(connected_realm_id: int = 1080):
    print("----- ENTERING AUCTION HOUSE RESOURCE FUNCTION -----")
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/connected-realm/{connected_realm_id}/auctions"
    params = {
        "namespace": "dynamic-eu",
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    # Yield the relevant data
    for auction in data.get("auctions", []):
        yield auction
    
    print("----------------------------------------")


# ---------- Function to fetch item class indexes from the World of Warcraft API ----------
@dlt.resource(write_disposition="replace", name="wow_item_class_indexes")
def wow_item_class_indexes():
    print("----- ENTERING ITEM CLASS INDEX RESOURCE FUNCTION -----")
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/item-class/index"
    params = {
        "namespace": "static-eu",
    }

        # Fetch the results using the helper function
    data = _get_results(url, headers, params)

    results = data.get("item_classes", [])

    # Yield the relevant data
    for item_class in results:
        yield item_class
    
    print("----------------------------------------")


# ---------- Function to fetch item data with pagination ----------
@dlt.resource(write_disposition="merge", name="wow_items", primary_key="id")
def wow_item_resource():
    print("----- ENTERING ITEM RESOURCE FUNCTION -----")
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    url = f"https://eu.api.blizzard.com/data/wow/search/item"
    params = {
        # ":region": "eu",
        "namespace": "static-eu",
        "orderby": "id",
        "_page": 1,
        "item_class.name.en_US": "Weapon",      # Filter for items in the Weapon class
        "item_subclass.name.en_US": "Sword",    # Filter for items in the Sword subclass
        "quality.name.en_US": "Legendary",      # Filter for items with Legendary quality
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
    
    print("----------------------------------------")
