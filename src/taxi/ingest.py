"""Download one month of NYC yellow taxi Parquet + the zone lookup CSV.

Usage (from repo root, with venv active):
    python -m src.taxi.ingest
"""

from __future__ import annotations

import os
from pathlib import Path

import requests
from dotenv import load_dotenv

TRIP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year_month}.parquet"
)
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/" "taxi_zone_lookup.csv"


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"Already have {dest} ({dest.stat().st_size:,} bytes)")
        return dest
    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
    print(f"Saved {dest} ({dest.stat().st_size:,} bytes)")
    return dest


def main() -> None:
    load_dotenv()
    year_month = os.getenv("TAXI_YEAR_MONTH", "2024-01")
    data_dir = Path(os.getenv("DATA_DIR", "data/raw"))

    trip_path = data_dir / f"yellow_tripdata_{year_month}.parquet"
    zone_path = data_dir / "taxi_zone_lookup.csv"

    download(TRIP_URL.format(year_month=year_month), trip_path)
    download(ZONE_URL, zone_path)
    print("Done.")


if __name__ == "__main__":
    main()
