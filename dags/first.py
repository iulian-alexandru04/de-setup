# dags/daily_weather_ingest.py
from __future__ import annotations

from datetime import datetime as dt, timedelta
from pathlib import Path
import json
import os
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

DEFAULT_OUT_DIR = "/opt/airflow/data/weather"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="daily_weather_ingest",
    description="Fetch daily weather from Open-Meteo and store as JSON (one file per run)",
    start_date=dt(2024, 1, 1),
    schedule="0 6 * * *",   # run daily at 06:00 UTC; adjust as you like
    catchup=False,
    default_args=default_args,
    params={
        # provide either a city OR explicit latitude/longitude
        "city": "Bucharest",
        "latitude": None,
        "longitude": None,
        # e.g. "Europe/Bucharest" for local; defaults to "UTC"
        "timezone": "UTC",
        # base folder to write into (mounted PVC, hostPath, etc.)
        "out_dir": DEFAULT_OUT_DIR,
    },
    max_active_runs=1,
    tags=["weather", "ingest", "open-meteo"],
)

with dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="resolve_location")
    def resolve_location(my_params: dict) -> dict:
        """
        Return {'city','latitude','longitude','timezone'}.
        If lat/lon are not provided, geocode the city via Open-Meteo's free geocoding API.
        """
        city = (my_params.get("city") or "").strip()
        lat = my_params.get("latitude")
        lon = my_params.get("longitude")
        tz = my_params.get("timezone") or "UTC"

        if lat is not None and lon is not None:
            return {"city": city or f"{lat},{lon}", "latitude": float(lat), "longitude": float(lon), "timezone": tz}

        if not city:
            raise ValueError("Provide either params.latitude/longitude or params.city")

        # Open-Meteo geocoding (no key needed)
        r = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city, "count": 1, "language": "en", "format": "json"},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        results = (data or {}).get("results") or []
        if not results:
            raise ValueError(f"Could not geocode city '{city}'")

        top = results[0]
        return {
            "city": top.get("name") or city,
            "latitude": float(top["latitude"]),
            "longitude": float(top["longitude"]),
            "timezone": tz,
        }

    @task(task_id="fetch_daily_weather")
    def fetch_daily_weather(loc: dict, ds: str) -> dict:
        """
        Fetch daily aggregates for execution date `ds` (YYYY-MM-DD) from Open-Meteo.
        """
        params = {
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
            "daily": ",".join(
                [
                    "weathercode",
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "apparent_temperature_max",
                    "apparent_temperature_min",
                    "precipitation_sum",
                    "sunrise",
                    "sunset",
                    "windspeed_10m_max",
                ]
            ),
            "timezone": loc["timezone"],
            # exact day of the run
            "start_date": ds,
            "end_date": ds,
        }
        r = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()

        # small, tidy envelope
        return {
            "metadata": {
                "source": "open-meteo",
                "fetched_at": dt.utcnow().isoformat(timespec="seconds") + "Z",
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
                "city": loc["city"],
                "timezone": loc["timezone"],
                "date": ds,
            },
            "data": payload,
        }

    @task(task_id="store_json")
    def store_json(envelope: dict, my_params: dict, ds: str) -> str:
        """
        Write the JSON to <out_dir>/<city>/<ds>.json and return the path.
        """
        city_safe = (envelope["metadata"]["city"] or "unknown").replace("/", "_").replace(" ", "_")
        out_dir = Path(my_params.get("out_dir") or DEFAULT_OUT_DIR) / city_safe
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{ds}.json"

        tmp_path = out_path.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(envelope, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, out_path)  # atomic-ish move

        return str(out_path)

    end = EmptyOperator(task_id="end")

    # wiring
    loc = resolve_location(dag.params)
    doc = fetch_daily_weather(loc)  # Airflow passes {{ ds }} automatically into task with param name 'ds'
    path = store_json(doc, dag.params)
    start >> loc >> doc >> path >> end

