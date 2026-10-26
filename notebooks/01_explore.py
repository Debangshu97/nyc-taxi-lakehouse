""" Phase 1: explore: schema, row count, null %, trips by day.

Run from repo root (venv active, data already ingested):
    python notebooks/01_explore.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    load_dotenv()
    year_month = os.getenv("TAXI_YEAR_MONTH", "2024-01")
    data_dir = Path(os.getenv("DATA_DIR", "data/raw"))
    trip_path = data_dir / f"yellow_tripdata_{year_month}.parquet"

    if not trip_path.exists():
        raise SystemExit(f"Missing {trip_path}. Run: python -m src.taxi.ingest")

    arrow_count = pq.read_table(trip_path).num_rows
    print(f"PyArrow row count: {arrow_count:,}")

    spark = (
        SparkSession.builder.appName("nyc-taxi-01-explore")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(trip_path))
    spark_count = df.count()
    print(f"Spark row count:   {spark_count:,}")

    if spark_count != arrow_count:
        raise SystemExit(
            f"FAIL: counts differ (Spark={spark_count}, PyArrow={arrow_count})"
        )
    print("PASS: Spark count == PyArrow count")

    print("\nSchema:")
    df.printSchema()

    total = spark_count
    print("\nNull % per column:")
    for col in df.columns:
        nulls = df.filter(F.col(col).isNull()).count()
        pct = 100.0 * nulls / total if total else 0.0
        print(f"  {col}: {pct:.2f}%")

    pickup = "tpep_pickup_datetime"
    if pickup in df.columns:
        print("\nTrips by day (first 10):")
        (
            df.withColumn("pickup_date", F.to_date(F.col(pickup)))
            .groupBy("pickup_date")
            .count()
            .orderBy("pickup_date")
            .show(10, truncate=False)
        )

    spark.stop()


if __name__ == "__main__":
    main()
