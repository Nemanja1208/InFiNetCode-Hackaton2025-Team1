import dlt
import auth_util
import json

# Fetch and bring all the connected realm IDs, returns a list of IDs
def _fetch_ids():
    """
    Fetches all the connected realm ids and cut out the id from the url. THIS IS ONLY NECESARY LIKE ONES?? 
    """
    endpoint = "/data/wow/connected-realm/index"
    params = {
        "namespace" : "dynamic-eu"
    }
    response = auth_util.get_api_response(endpoint=endpoint, params=params)
    connected_realm_list = response.json()["connected_realms"]
    
    list_of_realm_ids = [x["href"].split("/")[-1].split("?")[0] for x in connected_realm_list]
    return list_of_realm_ids


# Fetch data about connected realms    
@dlt.resource(table_name="realm_data", write_disposition="replace")
def fetch_realm_data():
    for realm_id in _fetch_ids():
        endpoint = f"/data/wow/connected-realm/{realm_id}"
        params = {                
            "{{connectedRealmId}}" : realm_id,
            "namespace" : "dynamic-eu",
            }
        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        response.raise_for_status()
        data = response.json()
        yield data
        print(f"Yieldat connected realm {realm_id}!")


# Fetch AH items
@dlt.resource(table_name="auctions", write_disposition="replace")
def fetch_auction_house_items():
    counter = 0 #DEBUG
    for realm_id in _fetch_ids(): 
        try:
            endpoint = f"/data/wow/connected-realm/{realm_id}/auctions"                             
            params = {                
                "{{connectedRealmId}}" : realm_id,
                "namespace" : "dynamic-eu",
                }
            response = auth_util.get_api_response(endpoint=endpoint, params=params)
            response.raise_for_status()
            counter += 1 #DEBUG
            data = response.json()
            for auction in data["auctions"]:
                auction["realm_id"] = realm_id
                yield auction # Detta tar rad för rad så i varje connected realm, så yieldar man istället rad för rad istället för alla cirka 30-50 k rader
            # yield data["auctions"] < Detta blir batchar man yieldar med 30 k rader svårt för dlt att behandla
        except Exception as e:
            print(f"DENNA MISSLYCKADES nr{counter} realm id: {realm_id}")
        print(f"\nYIELDAT NR {counter} <-------------------\n") #DEBUG


# Fetch AH commodities
@dlt.resource(table_name="commodities", write_disposition="replace") # merge?
def fetch_ah_commodities():
    endpoint = f"/data/wow/auctions/commodities"                             
    params = {                
        "namespace" : "dynamic-eu",
        }
    response = auth_util.get_api_response(endpoint=endpoint, params=params)
    data = response.json()
    for auction in data["auctions"]:
        yield auction


# Returns a list of indexes for item classes
def fetch_item_class_ids(): 
    endpoint = "/data/wow/item-class/index"
    params = {
        "namespace": "static-eu"
    }
    item_class_ids = []
    response = auth_util.get_api_response(endpoint=endpoint, params=params)
    data = response.json()
    for id in data.get("item_classes", []):
        print(id["id"])
        item_class_ids.append(id["id"])
    return item_class_ids

# Returns a dictionary of indexes for item subclasses
def fetch_item_subclasses_ids():
    item_class_ids = fetch_item_class_ids()
    subclass_dict = {}

    for item_class_id in item_class_ids:
        endpoint = f"/data/wow/item-class/{item_class_id}"
        params = {
            "namespace": "static-eu",
            "region": "eu"
        }

        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        data = response.json()

        subclass_ids = [subclass["id"] for subclass in data.get("item_subclasses", [])]
        subclass_dict[item_class_id] = subclass_ids

        print(f"Item class {item_class_id} has subclasses: {subclass_ids}")
    return subclass_dict
# Returns a dictionary with the itemclass names aswell as subclass names
def fetch_item_subclass_names():
    item_class_ids = fetch_item_class_ids()
    subclass_dict = {}

    for item_class_id in item_class_ids:
        endpoint = f"/data/wow/item-class/{item_class_id}"
        params = {
            "namespace": "static-eu",
            "region": "eu"
        }

        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        data = response.json()

        subclass_names = {subclass["id"]: subclass["name"]["en_US"] for subclass in data.get("item_subclasses", [])}
        subclass_dict[item_class_id] = subclass_names

        print(f"Item class {item_class_id} has subclasses: {subclass_names}")
    return subclass_dict


# Return all items, one rarity, class and subclass at a time
@dlt.resource(table_name="items", write_disposition="merge", primary_key="id")
def fetch_items():
    subclass_dict = fetch_item_subclasses_ids()
    all_items = []
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

    for item_class_id, subclass_ids in subclass_dict.items():
        for subclass_id in subclass_ids:
            for rarity in rarities:
                page = 1
                while True:
                    endpoint = "/data/wow/search/item"
                    params = {
                        "namespace": "static-eu",
                        "orderby": "id",
                        "_page": page,
                        "item_class.id": item_class_id,
                        "item_subclass.id": subclass_id,
                        "quality.name.en_US": rarity,
                    }

                    response = auth_util.get_api_response(endpoint=endpoint, params=params)
                    data = response.json()

                    results = data.get("results", [])
                    if not results:
                        break

                    for result in results:
                        yield result["data"]

                    print(f"Fetched {len(results)} items for class {item_class_id}, subclass {subclass_id}, page {page}")
                    
                    page += 1

                    if page > 10:
                        print(f"⚠️ Stopped after 10 pages for subclass {subclass_id}")
                        break

    print(f"✅ Fetched total {len(all_items)} items.")
    return all_items


# @dlt.source that we use in pipeline.run instead of @dlt.resource we use all the resources we want to run in the pipeline
@dlt.source(name="wow_api_data")
def wow_api_source():
    """
    This is the source function that will be used in the pipeline.
    It returns all the resources that we want to run in the pipeline.
    """
    return [fetch_items(),fetch_auction_house_items(),fetch_ah_commodities(),fetch_realm_data()]


if __name__ == "__main__":
    #fetch_ah_commodities()
    fetch_item_subclass_names()

