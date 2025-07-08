import dlt
from wow_api_dlt import auth_util,db
from wow_api_dlt.dlt_util import fetch_realm_ids, fetch_item_class_and_subclasses
import pandas as pd
import time
import sys 

# This is used to multithreading. We use it to perform multiple API calls
# in parallel, which can significantly speed up the data fetching process.
from concurrent.futures import ThreadPoolExecutor, as_completed 

# Fetch data about connected realms    
@dlt.resource(table_name="realm_data", write_disposition="replace")
def fetch_realm_data():
    for realm_id in fetch_realm_ids():
        endpoint = f"/data/wow/connected-realm/{realm_id}"
        params = {                
            "{{connectedRealmId}}" : realm_id,
            "namespace" : "dynamic-eu",
            }
        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        yield data
        print(f"Yielded connected realm ID: {realm_id}")


# Fetch AH items (excl. commodities)
@dlt.resource(table_name="auctions", write_disposition="replace")
def fetch_auction_house_items(test_mode=False):
    time_of_run = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"run started at : {time_of_run}")
    realm_ids = fetch_realm_ids() # Fetch all connected realm IDs once
    if test_mode:
        realm_ids = realm_ids[:3] # Limit for testing

    amount_of_realms = len(realm_ids)
    print(f"Total realms to fetch auction data for: {amount_of_realms}")

    # Adjust based on API rate limits and network latency for AH data
    MAX_WORKERS = 10 # 10 is a good value that avoids hitting limits

    current_processed_realms = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create an empty dictionary to map each future to its corresponding realm ID
        future_to_realm_id = {}

        # 1. Loop through all realm IDs that we want to fetch auction data for
        for r_id in realm_ids:
            # Construct the API endpoint and params for the current realm
            endpoint = f"/data/wow/connected-realm/{r_id}/auctions"
            params = {"{{connectedRealmId}}": r_id, "namespace": "dynamic-eu"}

            # Submit the API request to the thread pool executor
            # This starts the request in a background thread and returns a Future object immediately
            future = executor.submit(
                auth_util.get_api_response,  # The function to run in the thread
                endpoint=endpoint,           # The API endpoint
                params=params                # The parameters to include in the request
            )

            # Store the future along with the realm ID it represents
            # So later, when the future completes, we know which realm the response belongs to
            future_to_realm_id[future] = r_id

        _update_progress_bar(current_processed_realms, amount_of_realms, "Fetching AH Items")

        # 2. Start processing the futures as soon as they are completed (not in original submit order)
        for future in as_completed(future_to_realm_id):
            # Get the realm ID associated with this completed future
            realm_id = future_to_realm_id[future]
            
            try:
                # Get the result of the API call (this will block if not done, but it *is* done here)
                response = future.result()

                # Raise an error if the response was a 4xx or 5xx HTTP status
                response.raise_for_status()

                # Parse the JSON content of the response
                data = response.json()
                
                # If the expected "auctions" key is missing, print a warning and skip this realm
                if "auctions" not in data:
                    sys.stdout.write(f"\nWarning: 'auctions' key not found for realm ID: {realm_id}. Skipping.\n")
                    sys.stdout.flush()
                    continue

                # Loop through all auction entries returned for this realm
                for auction in data["auctions"]:
                    auction["realm_id"] = realm_id      # Add realm ID to each auction (to track source)
                    auction["timestamp"] = time_of_run  # Add timestamp of data pull
                    yield auction  # Yield the enriched auction record to the calling context

            except Exception as e:
                # If anything failed (network error, parsing, etc.), print the error
                # Printing on a new line avoids breaking the progress bar display
                sys.stdout.write(f"\nError fetching data for realm ID {realm_id}: {e}\n")
                sys.stdout.flush()
            
            # Increment the number of processed realms
            current_processed_realms += 1

            # Update the visual progress bar to show current progress
            _update_progress_bar(current_processed_realms, amount_of_realms, "Fetching AH Items")

    sys.stdout.write("\n") # Final newline after progress bar completion
    sys.stdout.flush()
    print(f"Finished fetching auction data for {current_processed_realms} realms.")


# Fetch AH commodities
@dlt.resource(table_name="commodities", write_disposition="append")
def fetch_ah_commodities():
    time_of_run = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"run started at : {time_of_run}")
    print("Fetching Auction House Commodities...")
    endpoint = "/data/wow/auctions/commodities"
    params = {
        "namespace": "dynamic-eu",
    }
    try:
        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        response.raise_for_status() # Check for HTTP errors
        data = response.json()
        if "auctions" not in data:
            print("Warning: 'auctions' key not found in commodities response. No data to yield.")
            return # Exit if no auctions
        for auction in data["auctions"]:
            auction["timestamp"] = time_of_run
            yield auction
        print("Successfully fetched Auction House Commodities.")
    except Exception as e:
        print(f"Error fetching Auction House Commodities: {e}")


# Fetch media url's
@dlt.resource(table_name="item_media", write_disposition="replace")
def fetch_media_hrfs():
    db_path = "wow_api_dbt/wow_api_data.duckdb"
    
    with db.DuckDBConnection(db_path) as db_handler:
        df = db_handler.query("SELECT DISTINCT media_id FROM refined.dim_items WHERE media_id IS NOT NULL")
        # df = df[:100] # For testing purposes, limit to 100 rows
    amount_of_media = len(df)
    current_media = 0
    for _, row in df.iterrows():
        try:
            media_id = int(row["media_id"])
        except (ValueError, TypeError):
            print(f"Skipping invalid media_id: {row['media_id']}")
            continue

        endpoint = f"/data/wow/media/item/{media_id}"
        params = {"namespace": "static-eu"}

        try:
            response = auth_util.get_api_response(endpoint=endpoint, params=params)
            data = response.json()
        except Exception as e:
            print(f"Failed to fetch or parse media {media_id}: {e}")
            continue

        assets = data.get("assets", [])
        if not isinstance(assets, list):
            print(f"Invalid assets format for media {media_id}")
            continue

        for asset in assets:
            if asset.get("key") == "icon":
                url = asset.get("value")
                if isinstance(url, str) and url.startswith("http"):
                    yield {
                        "media_id": media_id,
                        "url": url
                    }
                    current_media += 1
                    if current_media >= amount_of_media:
                        break
                else:
                    print(f"Invalid or missing URL for media {media_id}")
        print(f"Current media count: {current_media}/{amount_of_media}: {url}")


# Fetch item details
@dlt.resource()
def fetch_item_details():
    db_path = "wow_api_dbt/wow_api_data.duckdb"

    item_class_dict = fetch_item_class_and_subclasses()
    
    MAX_WORKERS = 10

    all_item_ids_to_fetch = []
    item_id_context = {} 

    print("Collecting item IDs from DuckDB...")
    for item_class_id, value in item_class_dict.items():
        item_class_name_raw = value.get("name", f"Unknown Class {item_class_id}")
        item_class_name_sanitized = item_class_name_raw.lower().replace(" ", "_").replace("-", "_")

        subclass_ids = value.get("subclass_ids", [])
        if not subclass_ids:
            subclass_ids = [None] 

        for subclass_id in subclass_ids:
            with db.DuckDBConnection(db_path) as db_handler:
                if subclass_id is not None:
                    query = f"SELECT DISTINCT id FROM refined.dim_items WHERE item_class_id = {item_class_id} AND item_subclass_id = {subclass_id}"
                else:
                    query = f"SELECT DISTINCT id FROM refined.dim_items WHERE item_class_id = {item_class_id}"
                
                df = db_handler.query(query)
                # df = df[:1] # Keep this commented out if you want to fetch all items for actual runs

            for _, row in df.iterrows():
                item_id = int(row["id"])
                all_item_ids_to_fetch.append(item_id)
                item_id_context[item_id] = { 
                    "item_class_id": item_class_id,
                    "class_name_sanitized": item_class_name_sanitized,
                    "class_name_raw": item_class_name_raw,
                    "subclass_id": subclass_id
                }

    amount_of_details = len(all_item_ids_to_fetch)
    print(f"Total unique items to fetch details for: {amount_of_details}")

    # Manual progress bar variables
    current_processed_count = 0
    bar_length = 50 # Length of the progress bar in characters
    
    # This allows us to use a thread pool to fetch item details concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_item_id = {
            executor.submit(auth_util.get_api_response, endpoint=f"/data/wow/item/{item_id}", params={"namespace": "static-eu"}): item_id
            for item_id in all_item_ids_to_fetch
        }

        # Initial display of the progress bar
        _update_progress_bar(current_processed_count, amount_of_details, bar_length)

        for future in as_completed(future_to_item_id):
            item_id = future_to_item_id[future]
            context = item_id_context[item_id]
            item_class_id = context["item_class_id"]
            item_class_name_sanitized = context["class_name_sanitized"]
            item_class_name_raw = context["class_name_raw"]
            subclass_id = context["subclass_id"]

            try:
                response = future.result() 
                data = response.json()

                if not isinstance(data, dict) or "id" not in data:
                    # Print full message on a new line for invalid data, then update progress
                    sys.stdout.write(f"\nWarning: Invalid data format for item {item_id}\n")
                    sys.stdout.flush()
                    continue

                item_data_to_yield = {}
                item_data_to_yield["id"] = data.get("id")
                if "description" in data:
                    item_data_to_yield["description"] = data.get("description", {}).get("en_US")
                if "binding" in data:
                    item_data_to_yield["binding_name"] = data.get("binding", {}).get("name")
                if "item_preview" in data:
                    item_data_to_yield["item_preview"] = data.get("item_preview")

                item_data_to_yield["item_class_id"] = item_class_id
                item_data_to_yield["item_subclass_id"] = subclass_id if subclass_id is not None else -1

                yield dlt.mark.with_table_name(item_data_to_yield, table_name=f"item_details_{item_class_name_sanitized}")

            except Exception as e:
                # Print full message on a new line for errors, then update progress
                sys.stdout.write(f"\nError fetching item {item_id}: {e}\n")
                sys.stdout.write(f"  Class: {item_class_name_raw}, Subclass: {subclass_id}\n")
                sys.stdout.flush()
                continue
            
            # Increment and update progress bar
            current_processed_count += 1
            _update_progress_bar(current_processed_count, amount_of_details, bar_length)

    # Final newline to ensure subsequent prints appear on a new line
    sys.stdout.write("\n")
    sys.stdout.flush()
    print(f"Finished fetching details for all items. Total processed: {current_processed_count} successfully.")


# UI function for updating progress bar during pipeline runs
def _update_progress_bar(current, total, desc, bar_length=50):
    """
    Manually updates a console progress bar on the same line.
    """
    if total == 0: # Avoid division by zero if no items
        sys.stdout.write(f"\r{desc}: No items to process.")
        sys.stdout.flush()
        return

    progress = current / total
    arrow = '=' * int(round(progress * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    
    sys.stdout.write(f"\r{desc}: [{arrow}{spaces}] {current}/{total} ({progress:.1%})")
    sys.stdout.flush()


# Fetch items
@dlt.resource(table_name="items", write_disposition="merge", primary_key="id")
def fetch_items():
    print("Starting item data extraction with adaptive filtering...")
    start_time = time.time()

    print("Fetching item subclasses...")
    item_class_dict = fetch_item_class_and_subclasses()
    print(f"✅ Item subclasses fetched. Found {len(item_class_dict)} item classes with subclasses.")

    rarities = [
        "Poor",        # Gray
        "Common",      # White
        "Uncommon",    # Green
        "Rare",        # Blue
        "Epic",        # Purple
        "Legendary",   # Orange
        "Artifact",    # Gold
        "Heirloom"     # Light Gold
    ]
    print(f"Configured with {len(rarities)} rarities: {', '.join(rarities)}")

    min_overall_ilvl = 1
    max_overall_ilvl = 750 # Covers current highest and projected TWW max ilvls with buffer
    step = 10

    all_level_ranges = [
        (start_ilvl, min(start_ilvl + step - 1, max_overall_ilvl))
        for start_ilvl in range(min_overall_ilvl, max_overall_ilvl + step, step)
    ]
    print(f"Pre-generated {len(all_level_ranges)} item level ranges for adaptive filtering.")

    total_items_fetched = 0
    total_api_calls = 0
    # New set to store (item_class_id, subclass_id) tuples that were not fully extracted
    incomplete_extractions = set()

    # Define the threshold for adaptive filtering
    ADAPTIVE_THRESHOLD_PAGES = 10
    API_RESULTS_PER_PAGE = 100 # Standard max results per page for WoW API
    # Page limit for refined queries (per ILvl range)
    REFINED_QUERY_PAGE_LIMIT = 10
    # Safety cap for initial broad search pages (to avoid excessively long broad searches)
    BROAD_SEARCH_SAFETY_CAP_PAGES = ADAPTIVE_THRESHOLD_PAGES + 10 # E.g., 20 pages total

    def _fetch_page_data(endpoint, params, current_page, filter_description):
        nonlocal total_api_calls
        params["_page"] = current_page
        print(f"          API Call: Page {current_page} for {filter_description}")
        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        total_api_calls += 1
        return response.json().get("results", [])

    class_count = len(item_class_dict)
    current_class_idx = 0

    # Starts looping through item class id's
    for item_class_id, value in item_class_dict.items():
        current_class_idx += 1
        print(f"\n--- Processing Item Class {item_class_id} ({current_class_idx}/{class_count}) ---")
        subclass_ids = value.get("subclass_ids", [])
        subclass_count = len(subclass_ids)
        current_subclass_idx = 0

        # Starts looping through item subclass id's
        for subclass_id in subclass_ids:
            current_subclass_idx += 1
            print(f"  Processing Subclass {subclass_id} ({current_subclass_idx}/{subclass_count}) within Class {item_class_id}")
            rarity_count = len(rarities)
            current_rarity_idx = 0

            # This flag tracks if any specific rarity/ilvl combination within this
            # class/subclass hit a page limit.
            subclass_extraction_complete = True

            # Starts looping through item rarities
            for rarity in rarities:
                current_rarity_idx += 1
                print(f"    Processing Rarity '{rarity}' ({current_rarity_idx}/{rarity_count}) for Subclass {subclass_id}")

                base_params = {
                    "namespace": "static-eu",
                    "orderby": "id",
                    "item_class.id": item_class_id,
                    "item_subclass.id": subclass_id,
                    "quality.name.en_US": rarity,
                }
                endpoint = "/data/wow/search/item"
                filter_desc = f"Class {item_class_id}, Subclass {subclass_id}, Rarity '{rarity}' (initial broad query)"

                items_in_initial_pass = 0
                page = 1
                should_refilter_by_ilvl = False
                current_filter_hit_page_limit = False # Flag for current rarity/ilvl combo

                # Attempt initial broad fetch
                while True:
                    results = _fetch_page_data(endpoint, base_params.copy(), page, filter_desc)

                    if not results:
                        print(f"            No more results for the broad filter. Breaking from page loop.")
                        break

                    # Check if we consistently get full pages up to the 10-page (1000 item) threshold.
                    if len(results) == API_RESULTS_PER_PAGE and page >= ADAPTIVE_THRESHOLD_PAGES:
                        should_refilter_by_ilvl = True
                        print(f"            Reached {page} full pages ({page * API_RESULTS_PER_PAGE} items). Will re-fetch this combo with item level filtering.")
                        break # Exit this broad while loop to proceed to ilvl filtering

                    for result in results:
                        yield result["data"]
                        total_items_fetched += 1
                        items_in_initial_pass += 1

                    print(f"            Fetched {len(results)} items on page {page}. Total for broad filter: {items_in_initial_pass}")

                    page += 1
                    # Safety cap for initial broad search pages. If hit, force refilter or acknowledge incomplete.
                    if page > BROAD_SEARCH_SAFETY_CAP_PAGES:
                        print(f"            ⚠️ Stopped broad search after {page-1} pages as a safety cap.")
                        current_filter_hit_page_limit = True # Mark as incomplete for this rarity/subclass
                        should_refilter_by_ilvl = True # Force refilter if too many pages
                        break # Exit broad while loop

                if should_refilter_by_ilvl:
                    print(f"      Initiating item level specific fetching for Class {item_class_id}, Subclass {subclass_id}, Rarity '{rarity}'")
                    items_in_refiltered_pass = 0
                    level_range_count = len(all_level_ranges)
                    current_level_range_idx = 0

                    for min_level, max_level in all_level_ranges:
                        current_level_range_idx += 1
                        print(f"        Fetching for Item Level Range: {min_level}-{max_level} ({current_level_range_idx}/{level_range_count})")
                        page = 1
                        items_in_current_ilvl_filter = 0

                        ilvl_params = base_params.copy()
                        ilvl_params["min_level"] = min_level
                        ilvl_params["max_level"] = max_level
                        ilvl_filter_desc = (
                            f"Class {item_class_id}, Subclass {subclass_id}, "
                            f"Rarity '{rarity}', ILvl {min_level}-{max_level}"
                        )

                        while True:
                            results = _fetch_page_data(endpoint, ilvl_params.copy(), page, ilvl_filter_desc)

                            if not results:
                                print(f"            No more results for ILvl {min_level}-{max_level}. Breaking from page loop.")
                                break

                            for result in results:
                                yield result["data"]
                                total_items_fetched += 1
                                items_in_refiltered_pass += 1
                                items_in_current_ilvl_filter += 1

                            print(f"            Fetched {len(results)} items on page {page}. Total for this ILvl filter: {items_in_current_ilvl_filter}")

                            page += 1
                            if page > REFINED_QUERY_PAGE_LIMIT:
                                print(f"            ⚠️ Stopped after {REFINED_QUERY_PAGE_LIMIT} pages for {ilvl_filter_desc}. This specific ILvl range may not be fully extracted.")
                                current_filter_hit_page_limit = True # Mark as incomplete for this specific ILvl range
                                break
                        print(f"        ✅ Completed fetching for ILvl Range {min_level}-{max_level}. Fetched {items_in_current_ilvl_filter} items.")
                    print(f"      ✅ Completed item level specific fetching. Total re-filtered: {items_in_refiltered_pass} items.")
                else:
                    print(f"      ✅ Completed broad fetching. Total fetched: {items_in_initial_pass} items. No item level refiltering needed.")

                if current_filter_hit_page_limit:
                    subclass_extraction_complete = False # If any rarity/ilvl hit a limit, the subclass is incomplete
                print(f"    ✅ Completed fetching for Rarity '{rarity}'.")

            # After all rarities for a subclass are processed, check if it was fully extracted
            if not subclass_extraction_complete:
                incomplete_extractions.add((item_class_id, subclass_id))
                print(f"  ❌ Subclass {subclass_id} within Class {item_class_id} was NOT fully extracted due to page limits.")
            else:
                print(f"  ✅ Subclass {subclass_id} within Class {item_class_id} fully extracted.")

        print(f"--- ✅ Completed processing for Item Class {item_class_id}. ---")

    end_time = time.time()
    duration = end_time - start_time

    print(f"\n--- Item Data Extraction Summary ---")
    print(f"✅ Fetched total {total_items_fetched} items.")
    print(f"✅ Made {total_api_calls} API calls.")
    print(f"✅ Total extraction time: {duration:.2f} seconds.")

    print(f"\n--- Incomplete Extractions Summary ---")
    if incomplete_extractions:
        print(f"❌ Found {len(incomplete_extractions)} item class/subclass combinations that were not fully extracted (hit page limits):")
        for class_id, sub_id in sorted(list(incomplete_extractions)): # Sort for consistent output
            print(f"  - Item Class: {class_id}, Subclass: {sub_id}")
        print("\nConsider increasing page limits (REFINED_QUERY_PAGE_LIMIT, BROAD_SEARCH_SAFETY_CAP_PAGES) or further refining filters for these combinations.")
    else:
        print("✅ All item class/subclass combinations appear to have been fully extracted within defined page limits.")
    print(f"------------------------------------")



# @dlt.source-decorated function that returns all the yields from the different resources
@dlt.source(name="wow_api_data")
def wow_api_source(optional_source_list=None,test_mode=False):
    """
    This is the source function that will be used in the pipeline.
    It returns all the resources that we want to run in the pipeline.

    If you want to use a source specific override for the pipeline you can add a list of resources to pick specific runs.
    accepted values are "auctions", "items" and "realm_data". If no list is provided, it will run all resources.
    """
    if optional_source_list is not None:
        # If an optional source dictionary is provided, we use it to pick resources
        method_list = []
        if "auctions" in optional_source_list:
            method_list.append(fetch_auction_house_items(test_mode=test_mode))
        if "commodities" in optional_source_list:
            method_list.append(fetch_ah_commodities())
        if "items" in optional_source_list:
            method_list.append(fetch_items())
        if "realm_data" in optional_source_list:
            method_list.append(fetch_realm_data())
        if "item_details" in optional_source_list:
            method_list.append(fetch_item_details())
        if "media" in optional_source_list:
            method_list.append(fetch_media_hrfs())
        return method_list
    else:
        #return [fetch_item_details()] 
        return [fetch_media_hrfs(),fetch_ah_commodities(),fetch_auction_house_items(),fetch_items(),fetch_realm_data(),fetch_item_details()] # # For testing purposes, we only run the media fetch resource


# For testing
if __name__ == "__main__":
    fetch_item_details()

