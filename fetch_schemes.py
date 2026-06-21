import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import pandas as pd 

def fetch_api_data(url):
    try:
        # 1. Always set a timeout to avoid hanging forever
        response = requests.get(url, timeout=40)
        
        # 2. Automatically raise HTTPError for 4xx and 5xx status codes
        response.raise_for_status()
        
        # 3. Handle data parsing after validating the response
        data = response.json()
        return data

    except HTTPError as http_err:
        # Handles 4xx (Client) and 5xx (Server) errors specifically
        status_code = http_err.response.status_code
        print(f"HTTP error occurred: {http_err}")
        
        if status_code == 401:
            print("Action required: Check your API keys or Authentication headers.")
        elif status_code == 404:
            print("Action required: The requested resource endpoint does not exist.")
        elif status_code == 429:
            print("Action required: Rate limit exceeded. Implement a back-off cooldown.")
            
    except ConnectionError as conn_err:
        # Handles physical network loss, DNS failures, or refused connections
        print(f"Connection error occurred: {conn_err}")
        print("Action required: Verify your internet connection or the server's domain.")
        
    except Timeout as timeout_err:
        # Handles requests that exceeded the specified timeout limit
        print(f"Timeout error occurred: {timeout_err}")
        print("Action required: Retry later or increase the timeout window.")
        
    except ValueError:
        # Handles cases where response.json() fails to parse valid JSON data
        print("Data error: The server returned a response, but it was not valid JSON.")
        
    except RequestException as req_err:
        # Catches any ambiguous exception that fell through the specific filters
        print(f"An unspecified requests error occurred: {req_err}")
        
    return None

# Execution example
api_url = "https://api.mfapi.in/mf"
result = fetch_api_data(api_url)

if result:
    df = pd.DataFrame(result)
    
    # 3. Save directly to CSV
    df.to_csv("schemes.csv", index=False, encoding="utf-8")
    
    # 4. Extract the schemeCodes as a clean Python list
    scheme_codes = df["schemeCode"].tolist()
    
    print(f"Saved {len(df)} entries to schemes.csv")
    print("First 5 scheme codes:", scheme_codes[:5])