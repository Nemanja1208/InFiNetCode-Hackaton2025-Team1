from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
import dlt
from dlt.sources.helpers.rest_client import RESTClient
BASE_URL = "https://eu.api.blizzard.com"


# Initialize the API client with secrets
def get_api_client():
    """
    Returns a RESTClient instance configured for the Blizzard API.
    """
    client_id = dlt.secrets["wow_api"]["client_id"]
    client_secret = dlt.secrets["wow_api"]["client_secret"]
    return RESTClient(
        base_url = BASE_URL,
        auth = OAuth2ClientCredentials(
            client_id = client_id,
            client_secret = client_secret,
            access_token_url = "https://oauth.battle.net/token",
        ),
    )


# Get response from the API
def get_api_response(endpoint: str, params: dict = {}):
    """
    Makes a request to the Blizzard API and returns the response.

    Args:
        endpoint (str): The API endpoint path.
        params (dict): Query parameters to include in the request.

    Returns:
        dict: The parsed response from the API.
    """
    client = get_api_client()
    response = client.get(path=endpoint, params=params)
    response.raise_for_status() # Ensure we raise an error for bad responses
    return response


# For testing
if __name__ == "__main__":
    pass
