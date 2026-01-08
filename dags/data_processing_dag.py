import os
import re
import pandas as pd
from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from utils import DATA_PATH, processed_dataset, check_file_empty, replace_nulls_func, sort_by_created_time, clean_content_func


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id="data_processing_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False
):
    
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=DATA_PATH,
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=600,
        mode="poke"
    )

    check_file = BranchPythonOperator(
        task_id = "check_file_content",
        python_callable=check_file_empty
    )

    empty_log = BashOperator(
        task_id="file_empty_branch",
        bash_command = "echo File is empty!"
    )

    with TaskGroup("processing_group") as processing_group:
        
        task_replace_nulls = PythonOperator(
            task_id='replace_nulls',
            python_callable=replace_nulls_func
        )

        task_sort_data = PythonOperator(
            task_id='sort_by_date',
            python_callable=sort_by_created_time
        )

        task_clean_content = PythonOperator(
            task_id='clean_content',
            python_callable=clean_content_func,
            outlets=[processed_dataset]
        )

        task_replace_nulls >> task_sort_data >> task_clean_content

    wait_for_file >> check_file
    check_file >> empty_log
    check_file >> processing_group
