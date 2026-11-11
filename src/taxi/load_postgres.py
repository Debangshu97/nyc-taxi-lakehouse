"""Gold Delta tables -> Postgres warehouse (taxi_dw) via JDBC."""

from __future__ import annotations

import os
from pathlib import Path

from src.taxi.spark_session import get_spark

GOLD_ROOT = Path(os.getenv("DELTA_ROOT", "data/delta")) / "gold"
TABLES = ["dim_zone", "dim_date", "fact_trip", "agg_daily_zone", "agg_hour_of_week"]

JDBC_URL = os.getenv("DW_JDBC_URL", "jdbc:postgresql://airflow-db:5432/taxi_dw")
PROPS = {
    "user": os.getenv("DW_USER", "airflow"),
    "password": os.getenv("DW_PASSWORD", "airflow"),
    "driver": "org.postgresql.Driver",
}


def main() -> None:
    spark = get_spark(
        "load-postgres", extra_packages=["org.postgresql:postgresql:42.7.4"]
    )
    for name in TABLES:
        df = spark.read.format("delta").load(str(GOLD_ROOT / name))
        df.repartition(2).write.mode("overwrite").option("batchsize", 5000).jdbc(
            JDBC_URL, f"public.{name}", properties=PROPS
        )
        print(f"{name}: {df.count():,} rows loaded")
    spark.stop()


if __name__ == "__main__":
    main()
