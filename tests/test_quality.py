

from datetime import datetime

from src.taxi.quality import is_valid_trip, with_reject_reason

COLS = ["fare_amount", "trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime"]


def make_df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_good_trip_passes(spark):
    df = make_df(spark, [(10.0, 2.5, datetime(2024, 1, 5, 10), datetime(2024, 1, 5, 10, 20))])
    assert df.filter(is_valid_trip(df)).count() == 1


def test_negative_fare_rejected(spark):
    df = make_df(spark, [(-5.0, 2.5, datetime(2024, 1, 5, 10), datetime(2024, 1, 5, 10, 20))])
    assert df.filter(is_valid_trip(df)).count() == 0
    reason = with_reject_reason(df).first()["reject_reason"]
    assert reason == "negative_fare"


def test_dropoff_before_pickup_rejected(spark):
    df = make_df(spark, [(10.0, 2.5, datetime(2024, 1, 5, 10, 20), datetime(2024, 1, 5, 10))])
    reason = with_reject_reason(df).first()["reject_reason"]
    assert reason == "dropoff_before_pickup"

def test_zero_distance_rejected(spark):
    df = make_df(spark, [(10.0, 0.0, datetime(2024, 1, 5, 10), datetime(2024, 1, 5, 10, 20))])
    assert df.filter(is_valid_trip(df)).count() == 0
    reason = with_reject_reason(df).first()["reject_reason"]
    assert reason == "nonpositive_distance"


def test_pickup_outside_month_rejected(spark):
    df = make_df(spark, [(10.0, 2.5, datetime(2024, 2, 3, 10), datetime(2024, 2, 3, 10, 20))])
    assert df.filter(is_valid_trip(df)).count() == 0
    reason = with_reject_reason(df).first()["reject_reason"]
    assert reason == "pickup_outside_month"