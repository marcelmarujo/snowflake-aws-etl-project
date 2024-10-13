# scripts/run_etl.py

import subprocess

def run_script(script_path):
    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

if __name__ == "__main__":
    print("Iniciando o processo ETL...")
    
    # Extração
    run_script('scripts/extract_api.py')
    run_script('scripts/extract_s3.py')
    run_script('scripts/extract_local_json.py')
    
    # Transformação
    run_script('scripts/transform.py')
    
    # Carregamento
    run_script('scripts/load_snowflake.py')
    
    print("Processo ETL concluído.")
