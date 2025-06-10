import requests
from dotenv import load_dotenv
import os

load_dotenv()

# Ensure you have the required environment variables set
CLIENT_ID = os.getenv("BLIZZARD_CLIENT_ID")
CLIENT_SECRET = os.getenv("BLIZZARD_CLIENT_SECRET")

# ---------- Function to get access token from Blizzard API ----------
def get_access_token():
    response = requests.post(
        url="https://oauth.battle.net/token",
        data={"grant_type": "client_credentials"},
        auth=(CLIENT_ID, CLIENT_SECRET),
    )
    response.raise_for_status()
    return response.json()["access_token"]
