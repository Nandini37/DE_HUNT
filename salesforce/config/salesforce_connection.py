
import requests
import os
from dotenv import load_dotenv
from pathlib import Path


# Load .env file from root directory
env_path = Path(__file__).parent.parent / '.env'  # Goes up from config/ to workspace/
load_dotenv(env_path)



# Parameters from your config
LOGIN_URL = "https://login.salesforce.com/services/oauth2/token"
CLIENT_ID = os.getenv('SALESFORCE_CLIENT_ID')
CLIENT_SECRET = os.getenv('SALESFORCE_CLIENT_SECRET')
USERNAME = os.getenv('SALESFORCE_USERNAME')
PASSWORD = os.getenv('SALESFORCE_PASSWORD')
SECURITY_TOKEN = os.getenv('SALESFORCE_SECURITY_TOKEN')

# 1. Get OAuth2 token
payload = {
    'grant_type': 'password',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'username': USERNAME,
    'password': PASSWORD,
}



response = requests.post(LOGIN_URL, data=payload)
response.raise_for_status()
auth_response = response.json()

access_token = auth_response['access_token']
instance_url = auth_response['instance_url']

print("Access token:", access_token)
print("Instance URL:", instance_url)

# 2. Example: Query current user info via REST API
headers = {
    'Authorization': f'Bearer {access_token}'
}
api_url = f"{instance_url}/services/data/v58.0/chatter/users/me"  # v58.0 as example

response = requests.get(api_url, headers=headers)
response.raise_for_status()
print(response.json())
print(response.status_code)

print(response.text)
print(response.headers)
print("completed")

# You can replace the API endpoint above with any Salesforce REST endpoint you need.
