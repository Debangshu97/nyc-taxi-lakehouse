"""Bronze → silver: quality split, MERGE, reject sink."""

from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.taxi.quality import is_valid_trip, with_reject_reason
from src.taxi.spark_session import get_spark


def main() -> None:
    bronze_path = Path("data/delta/bronze/trips")
    silver_path = Path("data/delta/silver/trips")
    rejects_path = Path("data/delta/silver/rejects")

    spark = get_spark("nyc-taxi-silver")
    bronze = spark.read.format("delta").load(str(bronze_path))

    # Stable key for MERGE (VendorID + pickup time + pickup location)
    keyed = bronze.withColumn(
        "trip_id",
        F.concat_ws(
            "|",
            F.col("VendorID").cast("string"),
            F.col("tpep_pickup_datetime").cast("string"),
            F.col("PULocationID").cast("string"),
        ),
    ).withColumn(
        "pickup_month",
        F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM"),
    )

    valid = keyed.filter(is_valid_trip(keyed)).dropDuplicates(["trip_id"])
    rejects = with_reject_reason(keyed.filter(~is_valid_trip(keyed)))

    if DeltaTable.isDeltaTable(spark, str(silver_path)):
        target = DeltaTable.forPath(spark, str(silver_path))
        (
            target.alias("t")
            .merge(valid.alias("s"), "t.trip_id = s.trip_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Silver MERGE into {silver_path}")
    else:
        (
            valid.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("pickup_month")
            .save(str(silver_path))
        )
        print(f"Silver created at {silver_path}")

    (
        rejects.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(rejects_path))
    )

    silver_n = spark.read.format("delta").load(str(silver_path)).count()
    reject_n = spark.read.format("delta").load(str(rejects_path)).count()
    print(f"Silver rows: {silver_n:,}")
    print(f"Reject rows: {reject_n:,}")
    print(f"Bronze check: {bronze.count():,}  (silver + reject should match)")
    spark.stop()


if __name__ == "__main__":
    main()
