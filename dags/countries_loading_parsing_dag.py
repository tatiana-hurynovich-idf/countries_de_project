from datetime import datetime, timedelta
from airflow.decorators import dag, task
from rest_api_operator import RestCountriesOperator
import json
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id = "countries_taskflow_pipeline",
    default_args = default_args,
    schedule = timedelta(days=1),
    start_date = datetime(2026, 10, 6),
    catchup = False
)
def countries_pipeline():
    extract_task = RestCountriesOperator(
        task_id = 'extract_and_parse_countries',
        api_key= os.getenv("REST_COUNTRIES_API_KEY"),
        base_url= 'https://api.restcountries.com/countries/v5'
    )

    @task 
    def save_data(countries_data: list, filepath: str = "/opt/airflow/data/countries.json"):

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
                json.dump(countries_data, f, ensure_ascii=False, indent=4)
                
        print(f"Data successfully saved to {filepath}. Total records: {len(countries_data)}")
        return filepath

    save_task = save_data(extract_task.output)


dag_instance = countries_pipeline()
