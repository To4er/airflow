import os
import re
import pandas as pd
from pymongo import MongoClient
from .config import DATA_PATH, OUTPUT_PATH, FIRST_TEMP_STEP, SECOND_TEMP_STEP

def check_file_empty(**kwargs):
    if os.stat(DATA_PATH).st_size == 0:
        return "file_empty_branch"
    return "processing_group.replace_nulls"


# Замена Nan и null на -
def replace_nulls_func():
    df = pd.read_csv(DATA_PATH)
    df.fillna('-', inplace=True)
    df.replace('null', '-', inplace=True)
    df.to_csv(FIRST_TEMP_STEP, index=False)


# Сортировка по created_time(в таблице этот параметр называется at)
def sort_by_created_time():
    df = pd.read_csv(FIRST_TEMP_STEP)
    df['at'] = pd.to_datetime(df['at'])
    df.sort_values('at', inplace=True)
    df.to_csv(SECOND_TEMP_STEP, index=False)


# Фильтрация контента
def clean_content_func():
    df = pd.read_csv(SECOND_TEMP_STEP)

    def clean_text(text):
        if not isinstance(text, str):
            return str(text)
        return re.sub(r'[^\w\s.,!?\'"-]', '', text)

    df['content'] = df['content'].apply(clean_text)

    df.to_csv(OUTPUT_PATH, index=False)

    if os.path.exists(FIRST_TEMP_STEP):
        os.remove(FIRST_TEMP_STEP)
    if os.path.exists(SECOND_TEMP_STEP):
        os.remove(SECOND_TEMP_STEP)


# Загрузка полученного датасета в бд
def load_to_mongo():
    client = MongoClient("mongodb://root:password@mongodb:27017/")
    db = client["airflow_db"]
    collection = db["processed_comments"]

    df = pd.read_csv(OUTPUT_PATH)
    data_dict = df.to_dict("records")

    collection.delete_many({})
    collection.insert_many(data_dict)
    print(f"Inserted {len(data_dict)} records into MongoDB")