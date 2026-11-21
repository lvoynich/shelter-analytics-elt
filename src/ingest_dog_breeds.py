"""
FILE:    src/ingest_dog_breeds.py
PURPOSE: Extracts canine breed metadata from The Dog API and loads it into
         Google BigQuery as a Bronze reference table (raw_dog_breeds).
"""

import logging
import os
from datetime import UTC, datetime
from typing import Any

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Configuration constants
API_URL = "https://" + "api.thedogapi" + ".com/v1/breeds"
PROJECT_ID = "project-29271b4d-441a-4f84-86c"
DATASET_ID = "animal_shelter"
TABLE_ID = "raw_dog_breeds"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


def create_http_session() -> requests.Session:
    """Create a requests Session configured with exponential backoff retries."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    api_key = os.getenv("DOG_API_KEY", "DEMO-API-KEY")
    session.headers.update({
        "x-api-key": api_key,
        "User-Agent": "shelter-analytics-elt/1.0",
    })

    return session


def fetch_dog_breeds() -> list[dict[str, Any]]:
    """Fetch all dog breed records from The Dog API."""
    logger.info("Fetching breed metadata from %s", API_URL)
    session = create_http_session()

    response = session.get(API_URL, timeout=15)
    response.raise_for_status()

    breeds = response.json()
    logger.info("Successfully retrieved %d breed records from API", len(breeds))
    return breeds


def transform_breed_records(raw_breeds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten nested JSON fields and append ingestion audit metadata."""
    ingested_at = datetime.now(UTC).isoformat()
    cleaned_records: list[dict[str, Any]] = []

    for item in raw_breeds:
        weight = item.get("weight") or {}
        height = item.get("height") or {}

        cleaned_records.append(
            {
                "breed_id": item.get("id"),
                "breed_name": item.get("name"),
                "breed_group": item.get("breed_group"),
                "bred_for": item.get("bred_for"),
                "life_span": item.get("life_span"),
                "temperament": item.get("temperament"),
                "origin": item.get("origin"),
                "weight_imperial": weight.get("imperial"),
                "weight_metric": weight.get("metric"),
                "height_imperial": height.get("imperial"),
                "height_metric": height.get("metric"),
                "ingested_at": ingested_at,
            }
        )

    return cleaned_records


def load_to_bigquery(records: list[dict[str, Any]]) -> None:
    """Load flattened breed records into BigQuery using an explicit schema."""
    client = bigquery.Client(project=PROJECT_ID)

    schema = [
        bigquery.SchemaField("breed_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("breed_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("breed_group", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("bred_for", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("life_span", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("temperament", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("origin", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("weight_imperial", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("weight_metric", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("height_imperial", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("height_metric", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info(
        "Loading %d records into BigQuery table %s", len(records), FULL_TABLE_ID
    )
    load_job = client.load_table_from_json(
        records, FULL_TABLE_ID, job_config=job_config
    )
    load_job.result()  # Wait for the load job to complete

    destination_table = client.get_table(FULL_TABLE_ID)
    logger.info(
        "Load complete! %s now has %d rows.", FULL_TABLE_ID, destination_table.num_rows
    )


def main() -> None:
    """Orchestrate the extract, flatten, and BigQuery load workflow."""
    raw_breeds = fetch_dog_breeds()
    transformed_records = transform_breed_records(raw_breeds)
    load_to_bigquery(transformed_records)


if __name__ == "__main__":
    main()
