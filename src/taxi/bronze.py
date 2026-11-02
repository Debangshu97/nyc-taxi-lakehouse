"""Write bronze Delta: raw taxi columns + lineage fields."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.taxi.spark_session import get_spark


def main() -> None:
    load_dotenv()
    year_month = os.getenv("TAXI_YEAR_MONTH", "2024-01")
    data_dir = Path(os.getenv("DATA_DIR", "data/raw"))
    trip_path = data_dir / f"yellow_tripdata_{year_month}.parquet"
    bronze_path = Path("data/delta/bronze/trips")

    if not trip_path.exists():
        raise SystemExit(f"Missing {trip_path}. Run: python -m src.taxi.ingest")

    spark = get_spark("nyc-taxi-bronze")
    df = spark.read.parquet(str(trip_path))

    bronze = df.withColumn("_source_file", F.lit(trip_path.name)).withColumn(
        "_ingested_at", F.current_timestamp()
    )

    (
        bronze.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(bronze_path))
    )

    print(f"Bronze written to {bronze_path}")
    print(
        f"Bronze row count: {spark.read.format('delta').load(str(bronze_path)).count():,}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
