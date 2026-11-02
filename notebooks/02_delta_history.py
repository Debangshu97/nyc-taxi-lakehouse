"""Show Delta history on silver trips (time travel proof)."""

from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable

from src.taxi.spark_session import get_spark


def main() -> None:
    silver_path = Path("data/delta/silver/trips")
    spark = get_spark("nyc-taxi-delta-history")
    dt = DeltaTable.forPath(spark, str(silver_path))
    history = dt.history(10).select(
        "version", "timestamp", "operation", "operationMetrics"
    )
    print("=== DESCRIBE HISTORY (silver trips) ===")
    history.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
