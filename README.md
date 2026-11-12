@"
# CurrencyFlow ETL: Airflow Pipeline (Frankfurter API to MongoDB)

**Full project report (setup, code and screenshots):** [Airflow_ETL_Report_23L0729.pdf](docs/Airflow_ETL_Report_23L0729.pdf)

## Overview
A daily Apache Airflow ETL pipeline (Astro CLI) that extracts EUR exchange rates from the Frankfurter API, transforms them into a fixed record format, and loads them into MongoDB. Data moves between tasks through named XComs. MongoDB and Mongo Express run as Docker services on a shared network, and MongoDB uses a persistent volume.

## Pipeline
extract_exchange_rates -> transform_exchange_rates -> load_to_mongodb

## Run locally
1. Create a ``.env`` file containing: ``MONGO_URI=mongodb://root:example@mongodb:27017/``
2. Run ``astro dev start``
3. Airflow UI: http://localhost:8080  |  Mongo Express: http://localhost:8081 (admin / admin123)
4. Trigger the DAG ``currency_exchange_etl``

## Key files
- ``dags/currency_etl_dag.py``: the DAG (extract, transform, load)
- ``docker-compose.override.yml``: MongoDB and Mongo Express services, network, volume
- ``requirements.txt``: Python dependencies (pymongo)

**Author:** Muhammad Fezaan Shamshad (23L-0729)
"@ | Set-Content -Encoding UTF8 README.md