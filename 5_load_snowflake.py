# scripts/load_snowflake.py

import snowflake.connector
import pandas as pd
import yaml

def load_config():
    with open('config/config.yaml', 'r') as file:
        return yaml.safe_load(file)

def get_snowflake_connection():
    config = load_config()
    snowflake_config = config['snowflake']
    
    conn = snowflake.connector.connect(
        user=snowflake_config['user'],
        password=snowflake_config['password'],
        account=snowflake_config['account'],
        warehouse=snowflake_config['warehouse'],
        database=snowflake_config['database'],
        schema=snowflake_config['schema']
    )
    return conn

def load_dataframe_to_snowflake(df, table_name, conn):
    # Usar método write_pandas para carregar DataFrame
    success, nchunks, nrows, _ = snowflake.connector.pandas_tools.write_pandas(
        conn, df, table_name.upper()
    )
    if success:
        print(f"Loaded {nrows} rows into {table_name}.")
    else:
        print(f"Failed to load data into {table_name}.")

if __name__ == "__main__":
    conn = get_snowflake_connection()
    
    # Carregar dados da API
    df_api = pd.read_csv('data/api_data_transformed.csv')
    load_dataframe_to_snowflake(df_api, 'market_data', conn)
    
    # Carregar dados do S3
    df_s3 = pd.read_csv('data/s3_data_transformed.csv')
    load_dataframe_to_snowflake(df_s3, 'market_data', conn)
    
    # Carregar dados dos arquivos JSON locais
    df_json = pd.read_csv('data/local_json_data_transformed.csv')
    load_dataframe_to_snowflake(df_json, 'market_data', conn)
    
    conn.close()
