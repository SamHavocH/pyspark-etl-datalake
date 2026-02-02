import requests

def extract_openmeteo(lat: float, lon: float, start_date: str, end_date: str) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()
