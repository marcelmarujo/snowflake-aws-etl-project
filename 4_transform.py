# scripts/transform.py

import pandas as pd

def transform_api_data(df):
    # Exemplo de transformação: filtrar colunas necessárias
    df = df[['symbol', 'price', 'timestamp']]
    return df

def transform_s3_data(df):
    # Exemplo de transformação: renomear colunas
    df = df.rename(columns={'ticker': 'symbol', 'last_price': 'price'})
    df = df[['symbol', 'price', 'timestamp']]
    return df

def transform_local_json_data(df):
    # Exemplo de transformação: preencher valores ausentes
    df = df.fillna('')
    df = df[['symbol', 'price', 'timestamp']]
    return df

if __name__ == "__main__":
    # Transformar dados da API
    df_api = pd.read_csv('data/api_data.csv')
    df_api_transformed = transform_api_data(df_api)
    df_api_transformed.to_csv('data/api_data_transformed.csv', index=False)
    print("API data transformed.")
    
    # Transformar dados do S3
    df_s3 = pd.read_csv('data/s3_data.csv')
    df_s3_transformed = transform_s3_data(df_s3)
    df_s3_transformed.to_csv('data/s3_data_transformed.csv', index=False)
    print("S3 data transformed.")
    
    # Transformar dados dos arquivos JSON locais
    df_json = pd.read_csv('data/local_json_data.csv')
    df_json_transformed = transform_local_json_data(df_json)
    df_json_transformed.to_csv('data/local_json_data_transformed.csv', index=False)
    print("Local JSON data transformed.")
