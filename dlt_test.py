import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dotenv import load_dotenv
import os
load_dotenv()

URL = f"https://eu.api.blizzard.com"

# LOGIK FÖR ATT HÄMTA EN LISTA PÅ ALLA connectionRealmIds
###
# [1001, 1003, 1080, 1122, ]
###

connectedRealmId = "1080" # Found it it Endpoint : Connected Realms Index 
                            # /data/wow/connected-realm/index
ah_endpoint = f"/data/wow/connected-realm/{connectedRealmId}/auctions"
item_endpoint = f""

def load_wow_ah(
        url,
        endpoint,   
        client_id: str = os.getenv("client_id"),
        client_secret: str = os.getenv("client_secret"),
    ) -> None:
    # Behind each keyword below, i will explain what it does
    pipeline = dlt.pipeline(
        pipeline_name="wow_ah",  # The name of the pipeline
        destination="duckdb",  # The destination where the data will be loaded
        dataset_name="raw"  # The name of the dataset
    )

# https://dlthub.com/docs/tutorial/rest-api

    wow_ah_source = rest_api_source(
        config={
            "client" : {
                "base_url" : url,
                "auth": OAuth2ClientCredentials(
                    client_id=client_id,
                    client_secret=client_secret,
                    access_token_url="https://oauth.battle.net/token",
                ),
            },
            "resources" : [
                {
                "write_disposition" : "replace",
                "name" : "items",
                "endpoint" : {
                    "params" : {
                        "namespace" : "static-eu",
                        "locale": "en_US",
                        "_page" : 1,
                        "orderby": "id",
                        "quality.name.en_US" : "Legendary"
                    },
                    "path" : "data/wow/search/item",
                },
                
                },
                {
                    "write_disposition" : "replace",
                    "name" : "auctions",
                    "endpoint" : {
                        "params" : {
                            "{{connectedRealmId}}" : connectedRealmId,
                            "namespace" : "dynamic-eu",
                            "locale": "en_US"
                        },
                        "path" : endpoint
                },
                },
            ]
            }
        )
    
    load_info = pipeline.run(wow_ah_source)
    print(load_info)

if __name__ == "__main__":
    load_wow_ah(url=URL, endpoint=ah_endpoint)