"""
CurrencyFlow ETL
Daily pipeline: Frankfurter API  ->  transform  ->  MongoDB
Data is passed between tasks with named XComs (explicit keys).
"""

import os
from datetime import datetime, timedelta, timezone

import requests
from pymongo import MongoClient, UpdateOne

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------------------- config
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017/")
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys
XCOM_RAW_RATES = "raw_exchange_rates"
XCOM_RETRIEVED_AT = "retrieved_at"
XCOM_TRANSFORMED = "transformed_exchange_rates"
XCOM_LOAD_SUMMARY = "load_summary"


# ---------------------------------------------------------------- tasks
def extract_exchange_rates(ti, **_):
    """Call the Frankfurter API and push the raw JSON to XCom."""
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()  # list of {date, base, quote, rate}

    if not raw_data:
        raise ValueError("Frankfurter API returned no rates")

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    ti.xcom_push(key=XCOM_RAW_RATES, value=raw_data)
    ti.xcom_push(key=XCOM_RETRIEVED_AT, value=retrieved_at)
    print(f"Extracted {len(raw_data)} rates at {retrieved_at}: {raw_data}")


def transform_exchange_rates(ti, **_):
    """Pull raw data, reshape each rate into the required format."""
    raw_data = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW_RATES)
    retrieved_at = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RETRIEVED_AT)

    transformed = []
    for item in raw_data:
        transformed.append(
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
        )

    ti.xcom_push(key=XCOM_TRANSFORMED, value=transformed)
    print(f"Transformed {len(transformed)} records: {transformed}")


def load_to_mongodb(ti, **_):
    """Pull transformed records and upsert them into MongoDB."""
    records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]

        # Upsert on (base, target, date) so re-running a day never creates duplicates
        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)

        summary = {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "matched": result.matched_count,
            "total_in_collection": collection.count_documents({}),
        }
    finally:
        client.close()

    ti.xcom_push(key=XCOM_LOAD_SUMMARY, value=summary)
    print(f"Loaded into {MONGO_DB}.{MONGO_COLLECTION}: {summary}")


# ---------------------------------------------------------------- DAG
default_args = {
    "owner": "FaizanNaroo",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API into MongoDB",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency", "mongodb"],
) as dag:

    extract = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_exchange_rates,
    )

    transform = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
    )

    load = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    extract >> transform >> load