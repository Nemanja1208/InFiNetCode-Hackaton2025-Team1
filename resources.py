
import dlt
from auth import get_access_token
from dotenv import load_dotenv
import requests
import os
load_dotenv()

@dlt.resource(
    name="resources",
    write_disposition="replace",
)
def load_ah_data():
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }
    base_url = "https://eu.api.blizzard.com"

    response = requests.get(
        f"{base_url}/data/wow/connected-realm/1080/auctions",
        headers=headers
    )
    response.raise_for_status()
    data = response.json()
    yield data["auctions"]


if __name__ == "__main__":
  pass
