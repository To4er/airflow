from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from utils import processed_dataset, load_to_mongo

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id="load_to_mongo",
    default_args=default_args,
    schedule=[processed_dataset],
):
    load_task = PythonOperator(
        task_id='load_csv_to_mongo',
        python_callable=load_to_mongo
    )