from datetime import datetime, timedelta, timezone
import requests

def extract_openmeteo(lat: float, lon: float, days_back: int) -> dict:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "timezone": "UTC",
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()
