# scripts/extract_local_json.py

import os
import json
import pandas as pd
import yaml
from datetime import datetime

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def extract_local_json():
    config = load_config()
    json_path = config['local_json_path']
    
    all_data = []
    for filename in os.listdir(json_path):
        if filename.startswith("marcel_financial_data_") and filename.endswith('.json'):
            filepath = os.path.join(json_path, filename)
            with open(filepath, 'r') as f:
                data = json.load(f)
                # Supondo que cada arquivo JSON contenha uma lista de registros
                for record in data:
                    record['timestamp'] = datetime.strptime(record['timestamp'], "%Y-%m-%d %H:%M:%S")
                all_data.extend(data)
    
    if all_data:
        df = pd.DataFrame(all_data)
        return df
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    df_json = extract_local_json()
    df_json.to_csv('data/local_json_data.csv', index=False)
    print("Local JSON data extracted and saved to data/local_json_data.csv")
