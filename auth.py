import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

load_dotenv()
# This module provides a function to get an access token from the Blizzard API using client credentials.
def get_access_token():
    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")

    response = requests.post(
        "https://oauth.battle.net/token",
        data={"grant_type": "client_credentials"}, 
        auth=HTTPBasicAuth(client_id, client_secret)
    )

    response.raise_for_status()
    return response.json()["access_token"] 