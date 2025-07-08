from wow_api_dlt import  auth_util

# Fetch and bring all the connected realm IDs, returns a list of IDs
def fetch_realm_ids():
    endpoint = "/data/wow/connected-realm/index"
    params = {
        "namespace" : "dynamic-eu"
    }
    response = auth_util.get_api_response(endpoint=endpoint, params=params)
    connected_realm_list = response.json()["connected_realms"]
    
    # Cuts the ID from the url, appending each ID to the list
    list_of_realm_ids = [x["href"].split("/")[-1].split("?")[0] for x in connected_realm_list]
    return list_of_realm_ids  


# Returns a dict of item classes, with another dict as value
def fetch_item_classes(): 
    item_class_dict = {}

    endpoint = "/data/wow/item-class/index"
    params = {
        "namespace": "static-eu"
    }
    response = auth_util.get_api_response(endpoint=endpoint, params=params)
    data = response.json()
    
    for item in data.get("item_classes", []):
        item_class_dict[item["id"]] = {"name": item["name"]["en_US"]}
    for key, value in item_class_dict.items():
        print(f"Item class ID: {key}, Name: {value['name']}")
    return item_class_dict


# Returns a dictionary of indexes for item subclasses
def fetch_item_class_and_subclasses():
    item_class_dict = fetch_item_classes()
    
    for item_class_id, value in item_class_dict.items():
        endpoint = f"/data/wow/item-class/{item_class_id}"
        params = {
            "namespace": "static-eu",
            "region": "eu"
        }

        response = auth_util.get_api_response(endpoint=endpoint, params=params)
        data = response.json()

        subclass_ids = [subclass["id"] for subclass in data.get("item_subclasses", [])]
        value["subclass_ids"] = subclass_ids

        print(f"Item class {value['name']} has subclasses: {subclass_ids}")
    return item_class_dict


# For testing
if __name__ == "__main__":
    fetch_item_classes()
