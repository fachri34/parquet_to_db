from airflow.decorators import dag, branch_task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import os
import json
import pandas as pd
import csv
import urllib.request

STAGING_AREA = os.path.join(os.getenv("AIRFLOW_HOME", ""), "data")
DB_NAME = os.path.join(STAGING_AREA, "airflow14.db")

default_args = {
    'owner': 'Opik',
    'start_date': days_ago(1)
}

def save_to_parquet(data, filename):
    base_filename = os.path.splitext(filename)[0]
    filepath = os.path.join(STAGING_AREA, f"{base_filename}.parquet")
    df = pd.DataFrame(data)
    df.to_parquet(filepath, engine='pyarrow')
    return filepath

def extract_from_csv(url, filename):
    filepath = os.path.join(STAGING_AREA, filename)
    urllib.request.urlretrieve(url, filepath)

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]
    
    parquet_file = save_to_parquet(data, filename)
    return parquet_file

def extract_from_json(url, filename):
    filepath = os.path.join(STAGING_AREA, filename)
    urllib.request.urlretrieve(url, filepath)

    with open(filepath, "r") as f:
        data = json.load(f)
    
    parquet_file = save_to_parquet(data, filename)
    return parquet_file

def load_to_sqlite(filename):
    engine = create_engine(f"sqlite:///{DB_NAME}")
    table_name = filename.split('.')[0]
    filepath = os.path.join(STAGING_AREA, filename)
    
    if filename.endswith('.parquet'):
        df = pd.read_parquet(filepath, engine='pyarrow')
        with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)

@dag()
def parquet_to_db():

    @branch_task
    def choose_extraction(**kwargs):
        source_type = kwargs['params'].get('source_type', 'csv')
        
        if source_type == 'csv':
            return 'extract_csv'
        elif source_type == 'json':
            return 'extract_json'
        else:
            return 'end_task'
        
    start_task = EmptyOperator(task_id="start_task")

    extract_csv = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_from_csv,
        op_kwargs={'url': 'https://raw.githubusercontent.com/codeforamerica/ohana-api/master/data/sample-csv/addresses.csv', 'filename': 'addresses.csv'},
    )
    
    extract_json = PythonOperator(
        task_id='extract_json',
        python_callable=extract_from_json,
        op_kwargs={'url': 'https://jsonplaceholder.typicode.com/posts', 'filename': 'posts.json'},
    )
    
    choose_extraction_task = choose_extraction()
    
    end_task = EmptyOperator(task_id="end_task")

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_sqlite,
        op_kwargs={
            'filename': '{{ task_instance.xcom_pull(task_ids="extract_csv") if dag_run.conf.get("source_type", "csv") == "csv" else task_instance.xcom_pull(task_ids="extract_json") }}'
        },
        trigger_rule='one_success'
    )
    
    start_task >> choose_extraction_task >> [extract_csv, extract_json] >> load_task >> end_task

parquet_to_db()
