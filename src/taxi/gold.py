"""Silver → gold star schema: dim_zone, dim_date, fact_trip."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.taxi.spark_session import get_spark


def main() -> None:
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "data/raw"))
    zone_path = data_dir / "taxi_zone_lookup.csv"
    delta_root = Path(os.getenv("DELTA_ROOT", "data/delta"))
    silver_path = delta_root / "silver/trips"
    gold_root = delta_root / "gold"

    spark = get_spark("nyc-taxi-gold")
    silver = spark.read.format("delta").load(str(silver_path))

    # --- dim_zone (grain: one row per LocationID) ---
    dim_zone = (
        spark.read.option("header", True)
        .csv(str(zone_path))
        .withColumnRenamed("LocationID", "zone_id")
        .withColumn("zone_id", F.col("zone_id").cast("int"))
        .select("zone_id", "Borough", "Zone", "service_zone")
        .dropDuplicates(["zone_id"])
    )
    dim_zone_path = gold_root / "dim_zone"
    (
        dim_zone.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(dim_zone_path))
    )

    # --- dim_date (grain: one row per calendar day in the silver range) ---
    dates = (
        silver.select(F.to_date("tpep_pickup_datetime").alias("date_day"))
        .dropna()
        .distinct()
    )
    dim_date = (
        dates.withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date_day"))
        .withColumn("month", F.month("date_day"))
        .withColumn("day", F.dayofmonth("date_day"))
        .withColumn("day_of_week", F.dayofweek("date_day"))  # 1=Sun … 7=Sat
        .withColumn("week_of_year", F.weekofyear("date_day"))
        .orderBy("date_day")
    )
    dim_date_path = gold_root / "dim_date"
    (
        dim_date.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(dim_date_path))
    )

    # --- fact_trip (grain: one row per trip_id) ---
    fact = (
        silver.withColumn("date_day", F.to_date("tpep_pickup_datetime"))
        .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
        .withColumn(
            "duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.col("tip_amount") / F.col("fare_amount"),
            ),
        )
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .select(
            "trip_id",
            "date_key",
            F.col("PULocationID").alias("pu_zone_id"),
            F.col("DOLocationID").alias("do_zone_id"),
            "VendorID",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "duration_minutes",
            "tip_pct",
            "hour_of_day",
            "day_of_week",
            "pickup_month",
        )
    )
    fact_path = gold_root / "fact_trip"
    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_month")
        .save(str(fact_path))
    )
    # --- aggregates from fact_trip ---
    # grain: one row per pickup date × pickup zone
    agg_daily_zone = fact.groupBy("date_key", "pu_zone_id").agg(
        F.count("*").alias("trips"),
        F.sum("fare_amount").alias("fare_total"),
        F.sum("total_amount").alias("revenue_total"),
        F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"),
        F.round(F.percentile_approx("duration_minutes", 0.5), 2).alias(
            "median_duration_min"
        ),
    )
    (
        agg_daily_zone.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(gold_root / "agg_daily_zone"))
    )

    # grain: one row per day_of_week × hour_of_day (heatmap-ready)
    agg_hour_of_week = fact.groupBy("day_of_week", "hour_of_day").agg(
        F.count("*").alias("trips"),
        F.sum("fare_amount").alias("fare_total"),
        F.round(F.avg("tip_pct"), 4).alias("avg_tip_pct"),
        F.round(F.percentile_approx("duration_minutes", 0.5), 2).alias(
            "median_duration_min"
        ),
    )
    (
        agg_hour_of_week.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(gold_root / "agg_hour_of_week"))
    )

    print(f"agg_daily_zone rows: {agg_daily_zone.count():,}")
    print(f"agg_hour_of_week rows: {agg_hour_of_week.count():,}")

    silver_n = silver.count()
    fact_n = spark.read.format("delta").load(str(fact_path)).count()
    silver_fare = silver.agg(F.sum("fare_amount").alias("s")).collect()[0]["s"]
    gold_fare = (
        spark.read.format("delta")
        .load(str(fact_path))
        .agg(F.sum("fare_amount").alias("s"))
        .collect()[0]["s"]
    )
    print(
        f"dim_zone rows: {spark.read.format('delta').load(str(dim_zone_path)).count()}"
    )
    print(
        f"dim_date rows: {spark.read.format('delta').load(str(dim_date_path)).count()}"
    )
    print(f"fact_trip rows: {fact_n:,}  (silver: {silver_n:,})")
    print(f"fare reconcile silver={silver_fare:.2f} gold={gold_fare:.2f}")
    spark.stop()


if __name__ == "__main__":
    main()
