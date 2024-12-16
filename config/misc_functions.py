import os, pickle  

import pandas as pd 

from google_auth_oauthlib.flow      import InstalledAppFlow 
from googleapiclient.discovery      import build 
from google.auth.transport.requests import Request

def get_authenticated_service(client_secret_file: str, api_service_name: str, api_version: str, pickle_file_name: str, scopes: list[str]):
    credentials = None

    if (os.path.exists(pickle_file_name)):
        with open(pickle_file_name, "rb") as token:
            credentials = pickle.load(token)
    
    if (not credentials or not credentials.valid):
        if ((credentials and credentials.expired) and (credentials.refresh_token)):
            credentials.refresh(Request())
        else:
            flow        = InstalledAppFlow.from_client_secrets_file(client_secret_file, scopes)
            credentials = flow.run_local_server(port = 0)
        
        with open(pickle_file_name, "wb") as token:
            pickle.dump(credentials, token)
    
    return build(api_service_name, api_version, credentials = credentials)
