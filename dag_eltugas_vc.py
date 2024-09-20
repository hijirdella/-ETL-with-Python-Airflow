from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from sqlalchemy import create_engine
import os
import json
import urllib.request
import pandas as pd
from datetime import datetime, timedelta
from airflow.models.param import Param

# Ensure the data directory exists
STAGING_DIR = "/opt/airflow/data/"
os.makedirs(STAGING_DIR, exist_ok=True)

@dag(
    params={
        "file_type": Param("csv", description="Type of file to extract (csv or json)"),
        "csv_url": Param("https://raw.githubusercontent.com/codeforamerica/ohana-api/master/data/sample-csv/addresses.csv", description="URL for the CSV file"),
        "json_url": Param("https://jsonplaceholder.typicode.com/posts", description="URL for the JSON file"),
        "csv_filename": Param("addresses.csv", description="Filename for the CSV file"),
        "json_filename": Param("posts.json", description="Filename for the JSON file"),
        "db_filename": Param("dbtugas.db", description="SQLite database filename")
    },
    schedule_interval='@daily',
    start_date=datetime(2024, 8, 2),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def dag_eltugas_vc():

    @task(task_id='extract_from_csv_task')
    def extract_from_csv(url, filename):
        local_filename = os.path.join(STAGING_DIR, filename)
        try:
            urllib.request.urlretrieve(url, local_filename)
        except urllib.error.HTTPError as e:
            print(f"Failed to download file from URL {url}. HTTP Error: {e.code}")
            raise
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

        if not os.path.exists(local_filename):
            raise FileNotFoundError(f"File {local_filename} was not downloaded successfully.")

        df = pd.read_csv(local_filename)
        staging_filename = os.path.join(STAGING_DIR, f"staging_{filename}")
        df.to_csv(staging_filename, index=False)
        return staging_filename

    @task(task_id='extract_from_json_task', trigger_rule='none_failed')
    def extract_from_json(url, filename):
        local_filename = os.path.join(STAGING_DIR, filename)
        try:
            urllib.request.urlretrieve(url, local_filename)
        except urllib.error.HTTPError as e:
            print(f"Failed to download file from URL {url}. HTTP Error: {e.code}")
            raise
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

        if not os.path.exists(local_filename):
            raise FileNotFoundError(f"File {local_filename} was not downloaded successfully.")

        with open(local_filename, 'r') as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        staging_filename = os.path.join(STAGING_DIR, f"staging_{filename.replace('.json', '.csv')}")
        df.to_csv(staging_filename, index=False)
        return staging_filename

    @task(task_id='load_to_sqlite_task', trigger_rule='all_done')
    def load_to_sqlite(staging_filename, db_filename):
        engine = create_engine(f"sqlite:///{STAGING_DIR}{db_filename}")
        df = pd.read_csv(staging_filename)
        table_name = os.path.splitext(os.path.basename(staging_filename))[0]
        with engine.connect() as conn:
            df.to_sql(table_name, conn, index=False, if_exists='replace')
        print(f"Data loaded into SQLite table: {table_name}")

    def choose_extract_task(**context):
        params = context['params']
        file_type = params['file_type']
        if file_type == 'csv':
            return 'extract_from_csv_task'
        elif file_type == 'json':
            return 'extract_from_json_task'
        else:
            return 'end_task'

    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task', trigger_rule='none_failed')

    choose_extract_task_op = BranchPythonOperator(
        task_id='choose_extract_task',
        python_callable=choose_extract_task,
        provide_context=True
    )

    extract_from_csv_task = extract_from_csv(
        url="{{ params.csv_url }}",
        filename="{{ params.csv_filename }}"
    )

    extract_from_json_task = extract_from_json(
        url="{{ params.json_url }}",
        filename="{{ params.json_filename }}"
    )

    load_to_sqlite_task = load_to_sqlite(
        staging_filename="{{ task_instance.xcom_pull(task_ids='extract_from_csv_task') if params['file_type'] == 'csv' else task_instance.xcom_pull(task_ids='extract_from_json_task') }}",
        db_filename="{{ params.db_filename }}"
    )

    start_task >> choose_extract_task_op
    choose_extract_task_op >> [extract_from_csv_task, extract_from_json_task] >> load_to_sqlite_task
    load_to_sqlite_task >> end_task

dag_eltugas_vc()
