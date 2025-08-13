import requests
import json
from requests.auth import HTTPBasicAuth

def generate_dropbox_access_token(app_key, app_secret, refresh_token):

    url = "https://api.dropbox.com/oauth2/token"

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    response = requests.post(
        url,
        data=data,
        auth=HTTPBasicAuth(app_key, app_secret)
    )

    return response.json()["access_token"]

def file_exists(filename, search_path="/", access_token=None):

    url = "https://api.dropboxapi.com/2/files/search_v2"

    payload = {
        "query": filename,
        "path": search_path,     
        "max_results": 1,
        "filename_only": True
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        matches = response.json().get("matches", [])
        if matches:
            metadata = matches[0]["metadata"]["metadata"]
            return True, metadata["path_display"]
        return False, None
    else:
        raise Exception(f"Dropbox API error {response.status_code}: {response.text}")
    
def download_backup_file(file_path, download_path, access_token=None):

    url = 'https://content.dropboxapi.com/2/files/download'

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({"path": file_path})
    }

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        with open(download_path, "wb") as f:
            f.write(response.content)
        print("ZIP file downloaded successfully.")
    else:
        print(f"Failed to download. Status code: {response.status_code}")