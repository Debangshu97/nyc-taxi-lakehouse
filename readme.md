# nyc-taxi-lakehouse

Personal learning project: an end-to-end lakehouse on public NYC yellow taxi data (ingest → PySpark explore → later Delta bronze/silver/gold → Airflow → Power BI).

## Status

**Phase 1 done (local + Databricks Free Edition)**
- Downloaded Jan 2024 yellow taxi Parquet + zone lookup
- Local explore: Spark row count == PyArrow row count (**2,964,624**)
- Null % documented in `docs/data_dictionary.md`
- Same explore run in Databricks Free Edition (screenshot in `docs/img/`)

## How to run (Phase 1)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
copy .env.example .env
python -m src.taxi.ingest
python notebooks/01_explore.py