import requests

# Parameters from your config
LOGIN_URL = "https://login.salesforce.com/services/oauth2/token"
CLIENT_ID = "xxxxxxxxx"
CLIENT_SECRET = "xxxx"
USERNAME = "xx@"
PASSWORD = "abbbbccc"
SECURITY_TOKEN = "ggggg"

# 1. Get OAuth2 token
payload = {
    'grant_type': 'password',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'username': USERNAME,
    'password': PASSWORD + SECURITY_TOKEN
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

# You can replace the API endpoint above with any Salesforce REST endpoint you need.
