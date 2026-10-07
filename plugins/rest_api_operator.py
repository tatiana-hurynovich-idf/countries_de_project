from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.models import BaseOperator

class RestCountriesOperator(BaseOperator):
    def __init__(self, api_key: str, base_url: str, dag=None, **kwargs):
        super().__init__(dag=dag, **kwargs)
        self.api_key = api_key
        self.base_url = base_url

    def execute(self, context):
        self.log.info("Starting data extraction from REST Countries API...")

        if not self.api_key:
            raise ValueError(
                "Error: API key not found! Please check your configuration."
            )

        headers = {"Authorization": f"Bearer {self.api_key}"}
        all_objects = []
        limit = 100
        offset = 0
        more_data = True

        response_fields = (
            "names.common,"
            "codes.alpha_2,"
            "capital,"
            "region,"
            "subregion,"
            "population,"
            "area,"
            "landlocked,"
            "flag.emoji"
        )

        # грузим данные 

        while more_data:
            url = f"{self.base_url}?response_fields={response_fields}&limit={limit}&offset={offset}"
            self.log.info(f"Fetching data: offset = {offset}, limit = {limit}...")

            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()

                res_json = response.json()
                data_wrapper = res_json.get("data", {})
                objects = data_wrapper.get("objects", [])
                meta = data_wrapper.get("meta", {})

                if objects:
                    all_objects.extend(objects)

                more_data = meta.get("more", False)
                offset += limit

            except requests.exceptions.RequestException as e:
                self.log.error(f"An error occurred during the API request: {e}")
                if hasattr(e, "response") and e.response is not None:
                    self.log.error(f"Server response: {e.response.text}")
                raise e

        self.log.info(f"Total raw objects fetched: {len(all_objects)}")


        # парсим данные
        parsed_countries = []
        for country in all_objects:
            name = country.get("names", {}).get("common", "Unknown")
            alpha_2 = country.get("codes", {}).get("alpha_2", "Unknown")
            capital_raw = country.get("capital", [])
            capital = (
                capital_raw[0]
                if isinstance(capital_raw, list) and capital_raw
                else "No capital"
            )

            region = country.get("region", "Unknown")
            subregion = country.get("subregion", "Unknown")
            population = country.get("population", 0)
            area = country.get("area", 0.0)
            landlocked = country.get("landlocked", False)
            flag_emoji = country.get("flag", {}).get("emoji", "")

            parsed_countries.append({
                "name": name,
                "alpha_2": alpha_2,
                "capital": capital,
                "region": region,
                "subregion": subregion,
                "population": population,
                "area_sq_km": area,
                "is_landlocked": landlocked,
                "flag_emoji": flag_emoji,
            })

        self.log.info(
            f"Successfully parsed {len(parsed_countries)} countries."
        )

        return parsed_countries