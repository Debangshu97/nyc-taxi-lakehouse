# Data dictionary (v0)

Source file: `yellow_tripdata_2024-01.parquet`  
Row count (Spark = PyArrow): **2,964,624**

## Null % per column

| Column | Null % |
|--------|--------|
| VendorID | 0.00% |
| tpep_pickup_datetime | 0.00% |
| tpep_dropoff_datetime | 0.00% |
| passenger_count | 4.73% |
| trip_distance | 0.00% |
| RatecodeID | 4.73% |
| store_and_fwd_flag | 4.73% |
| PULocationID | 0.00% |
| DOLocationID | 0.00% |
| payment_type | 0.00% |
| fare_amount | 0.00% |
| extra | 0.00% |
| mta_tax | 0.00% |
| tip_amount | 0.00% |
| tolls_amount | 0.00% |
| improvement_surcharge | 0.00% |
| total_amount | 0.00% |
| congestion_surcharge | 4.73% |
| Airport_fee | 4.73% |

## Notes
- A few rows have pickup dates outside Jan 2024 (seen in trips-by-day). Flag for silver rules.

## Gold layer (Phase 3)

| Table | Grain | Key columns | Notes |
|-------|-------|-------------|-------|
| dim_zone | one row per TLC LocationID | zone_id | From taxi_zone_lookup.csv |
| dim_date | one row per calendar day in silver | date_key (yyyyMMdd) | day_of_week: 1=Sun … 7=Sat (Spark) |
| fact_trip | one row per trip | trip_id | FKs: date_key, pu_zone_id, do_zone_id; partitioned by pickup_month |
| agg_daily_zone | one row per pickup date × pickup zone | date_key, pu_zone_id | trips, fare_total, revenue_total, avg_tip_pct, median_duration_min |
| agg_hour_of_week | one row per day_of_week × hour_of_day | day_of_week, hour_of_day | Heatmap-ready; up to 7×24 = 168 rows |

### Reconciliation

- fact_trip row count = silver trips row count
- sum(fact_trip.fare_amount) = sum(silver.fare_amount) (within rounding)

## Silver layer

**trip_id key:** `VendorID | tpep_pickup_datetime | PULocationID | tpep_dropoff_datetime | DOLocationID`

Dedup is deterministic: among rows with the same `trip_id`, keep the first by
`tpep_dropoff_datetime desc, total_amount desc, DOLocationID desc` (window `row_number`).

> Fix (Phase 5): the original key (vendor + pickup time + pickup zone) collided for
> 76,872 IDs, so `dropDuplicates` kept an arbitrary row and fare totals differed
> between Windows and Docker. Stronger key + ordered dedup made results identical.

## Row counts (Jan 2024)

| Layer | Rows |
|-------|------|
| bronze | 2,964,624 |
| silver | 2,869,971 |
| rejects | 94,565 |
| exact duplicates removed | 88 |
| fact_trip | 2,869,971 |

Reconcile: silver + rejects + duplicates = bronze.
Fare total silver = gold = **53,083,484.48** (same on Windows and in Docker).