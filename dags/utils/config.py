from airflow import Dataset


DATA_PATH = "/opt/airflow/data/airflow/tiktok_google_play_reviews.csv"
OUTPUT_PATH = "/opt/airflow/data/airflow/filtered_data.csv"
MONGO_CONN_ID = 'mongo_default'
FIRST_TEMP_STEP = '/opt/airflow/data/temp_step_1.csv'
SECOND_TEMP_STEP = '/opt/airflow/data/temp_step_2.csv'

processed_dataset = Dataset(f"file://{OUTPUT_PATH}")