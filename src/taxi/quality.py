"""Silver quality rules for yellow taxi trips."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def is_valid_trip(df: DataFrame) -> Column:
    """True when a row passes all silver quality checks."""
    return (
        (F.col("fare_amount") >= 0)
        & (F.col("trip_distance") > 0)
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (
            F.month(F.col("tpep_pickup_datetime"))
            == F.month(F.lit("2024-01-01").cast("date"))
        )
        & (
            F.year(F.col("tpep_pickup_datetime"))
            == F.year(F.lit("2024-01-01").cast("date"))
        )
    )


def with_reject_reason(df: DataFrame) -> DataFrame:
    """Add reject_reason for rows that fail quality (first failing rule wins)."""
    reason = (
        F.when(F.col("fare_amount") < 0, F.lit("negative_fare"))
        .when(F.col("trip_distance") <= 0, F.lit("nonpositive_distance"))
        .when(
            F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"),
            F.lit("dropoff_before_pickup"),
        )
        .when(
            (F.year(F.col("tpep_pickup_datetime")) != 2024)
            | (F.month(F.col("tpep_pickup_datetime")) != 1),
            F.lit("pickup_outside_month"),
        )
        .otherwise(F.lit("unknown"))
    )
    return df.withColumn("reject_reason", reason)
