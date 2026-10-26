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