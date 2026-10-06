import json
import os
from dotenv import load_dotenv
import requests


load_dotenv()

API_KEY = os.getenv("REST_COUNTRIES_API_KEY")
BASE_URL = "https://api.restcountries.com/countries/v5"


def fetch_all_countries() -> list:
  """Fetches all countries from the API using pagination (offset / limit)."""
  if not API_KEY:
    print(
        "Error: API key not found in environment variables (.env)! Please check"
        " your configuration."
    )
    return []

  headers = {"Authorization": f"Bearer {API_KEY}"}

  all_objects = []
  limit = 100  # 100 записей на один батч
  offset = 0
  more_data = True

  # Список полей
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

  print("Starting data extraction from REST Countries API...")

  while more_data:
    url = f"{BASE_URL}?response_fields={response_fields}&limit={limit}&offset={offset}"

    print(f"Fetching data: offset = {offset}, limit = {limit}...")
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
      print(f"An error occurred during the API request: {e}")
      if hasattr(e, "response") and e.response is not None:
        print(f"Server response: {e.response.text}")
      break

  return all_objects


def parse_countries(raw_objects: list) -> list:
  """Transforms raw API objects into a flat list of dictionaries with expanded fields."""
  parsed_countries = []

  for country in raw_objects:
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

  return parsed_countries


def save_to_json(data: list, filepath: str) -> None:
  """ сохр обработанные данные в json"""
  os.makedirs(os.path.dirname(filepath), exist_ok=True)

  with open(filepath, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
  print(
      f"Data successfully saved to {filepath}. Total records: {len(data)}"
  )


if __name__ == "__main__":
  raw_data = fetch_all_countries()
  if raw_data:
    cleaned_data = parse_countries(raw_data)
    save_to_json(cleaned_data, "data/countries.json")