# scripts/extract_s3.py

import boto3
import pandas as pd
import yaml
from io import StringIO

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def extract_s3_data():
    config = load_config()
    aws_config = config['aws']
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_config['access_key'],
        aws_secret_access_key=aws_config['secret_key']
    )
    
    bucket = aws_config['s3_bucket']
    # Listar objetos no bucket
    response = s3.list_objects_v2(Bucket=bucket)
    
    all_data = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.csv'):  # Supondo que os dados sejam CSV
            obj_response = s3.get_object(Bucket=bucket, Key=key)
            data = obj_response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(data))
            all_data.append(df)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    df_s3 = extract_s3_data()
    df_s3.to_csv('data/s3_data.csv', index=False)
    print("S3 data extracted and saved to data/s3_data.csv")
