# scripts/extract_api.py

import requests
import pandas as pd
import yaml
from datetime import datetime

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def extract_api_data():
    config = load_config()
    api_config = config['api']
    params = {
        "function": api_config['function'],
        "symbol": api_config['symbol'],
        "interval": api_config['interval'],
        "apikey": api_config['api_key'],
        "datatype": "json"
    }
    response = requests.get(api_config['endpoint'], params=params)
    
    if response.status_code == 200:
        data = response.json()
        time_series_key = f"Time Series ({api_config['interval']})"
        if time_series_key in data:
            time_series = data[time_series_key]
            records = []
            for timestamp, values in time_series.items():
                records.append({
                    "symbol": api_config['symbol'],
                    "price": float(values["4. close"]),
                    "timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                })
            df = pd.DataFrame(records)
            return df
        else:
            raise Exception("Unexpected API response structure.")
    else:
        raise Exception(f"Failed to fetch API data: {response.status_code}")

if __name__ == "__main__":
    df_api = extract_api_data()
    df_api.to_csv('data/api_data.csv', index=False)
    print("API data extracted and saved to data/api_data.csv")
