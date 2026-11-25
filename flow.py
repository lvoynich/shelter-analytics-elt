"""
Animal Shelter Operations & Stay Analytics — Orchestrated ELT Pipeline
======================================================================
Purpose:
    Coordinates the end-to-end Extract, Load, and Transform (ELT) workflow:
    1. Extracts raw intake/outcome records from the Animal Shelter REST API
       and loads them into the BigQuery Bronze (raw) layer.
    2. Executes dbt Core models and automated data quality tests to build
       the Silver (staging) and Gold (Kimball star schema) layers.

Why Prefect 3?
    - Decouples orchestration from business logic: Python handles API movement,
      SQL/dbt handles warehouse transformations, and Prefect manages execution
      order, retries, logging, and alerting.
    - Fail-fast protection: Downstream dbt models will NOT execute if upstream
      ingestion fails, preventing silent data corruption in Gold tables.
"""

from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

# ---------------------------------------------------------------------------
# Import your existing ingestion entry-point function here.
# Example: from src.ingest import run_ingestion
# Why import instead of rewriting? Keeping extraction logic in its own module
# makes it unit-testable with pytest without needing a Prefect server running.
# ---------------------------------------------------------------------------
# Imports src/ingest_dog_breeds.py as a Python module
from src import ingest_dog_breeds


@task(
    name="Ingest Dog Breeds API to BigQuery (Bronze)",
    retries=3,
    retry_delay_seconds=15,
    tags=["extract-load", "api", "bigquery-bronze"],
)
def extract_and_load_breeds() -> None:
    """
    Fetches 631 dog breed records from the REST API and loads them into BigQuery.

    Why call a Python function instead of `subprocess.run(["python", "src/ingest_dog_breeds.py"])`?
        - Native error handling: If the API fails or BigQuery rejects a row, Python
          raises the real exception directly to Prefect, triggering `retries=3`.
        - Shared process & logging: `log_prints=True` automatically captures any
          print() or logging statements inside ingest_dog_breeds.py in the Prefect UI.
    """
    logger = get_run_logger()
    logger.info("Starting REST API breed ingestion into BigQuery Bronze...")

    # Replace `main()` with whatever function runs your script in ingest_dog_breeds.py
    ingest_dog_breeds.main()

    logger.info("Successfully loaded dog breed reference data into BigQuery.")


@task(
    name="Extract & Load Shelter API to BigQuery (Bronze)",
    # WHY RETRIES? External REST APIs frequently suffer transient hiccups
    # (HTTP 429 rate limits, 502/503 gateway timeouts, or brief DNS drops).
    # Setting retries=3 tells Prefect to automatically re-attempt this task
    # up to 3 times before marking the pipeline as Failed.
    retries=3,
    # WHY RETRY DELAY? Immediate retries often hit the exact same rate limit
    # or server outage. A 15-second pause gives the upstream API breathing room.
    retry_delay_seconds=15,
    # WHY TAGS? Tags let you filter runs in the Prefect UI and set concurrency
    # limits later (e.g., ensuring two ingestion jobs don't hit an API at once).
    tags=["ingestion", "bigquery", "bronze"],
)
def extract_and_load_bronze() -> None:
    """
    Executes the Python REST API ingestion into BigQuery raw landing tables.

    Why this is a separate @task:
        In Prefect, a @task is the smallest unit of retryable work. By isolating
        API ingestion in its own task, if dbt fails later due to a SQL bug,
        we don't have to re-fetch data from the API to retry dbt.
    """
    logger = get_run_logger()
    logger.info("Starting REST API extraction to BigQuery Bronze layer...")

    # TODO: Call your existing ingestion function here, e.g.:
    # records_loaded = run_ingestion()
    # logger.info(f"Successfully loaded {records_loaded} records into Bronze.")

    logger.info("Bronze layer ingestion completed.")


@task(
    name="Transform & Test Warehouse Models (dbt Build)",
    # WHY NO RETRIES ON DBT? Unlike flaky external APIs, dbt failures are
    # usually caused by deterministic issues: a broken SQL query, a failed
    # data quality test (e.g., duplicate primary keys), or a schema change.
    # Retrying immediately wastes BigQuery compute credits without fixing the bug.
    retries=0,
    tags=["transformation", "dbt", "silver-gold"],
)
def transform_and_test_dbt(
    project_dir: Path,
    profiles_dir: Path,
    target: str = "dev",
) -> None:
    """
    Runs `dbt build` via the native prefect-dbt integration.

    Why `dbt build` instead of separate `dbt seed`, `dbt run`, and `dbt test`?
        `dbt build` executes seeds, models, and tests in DAG (dependency) order.
        If an upstream staging model fails a `not_null` or `unique` test, dbt
        immediately halts downstream Gold fact/dimension models so bad data
        never reaches end users.

    Why `PrefectDbtRunner` instead of Python's `subprocess.run(["dbt", ...])`?
        `PrefectDbtRunner` hooks directly into dbt's internal events, streaming
        structured logs into the Prefect UI and surfacing each dbt model and
        test as an observable node in your Prefect run graph.
    """
    logger = get_run_logger()
    logger.info(f"Initializing dbt runner for target environment: '{target}'")

    # PrefectDbtSettings locates your dbt_project.yml and profiles.yml.
    # Using Path objects ensures cross-platform compatibility across macOS
    # (local development) and Linux (inside your Docker container).
    settings = PrefectDbtSettings(
        project_dir=project_dir,
        profiles_dir=profiles_dir,
    )

    # raise_on_failure=True is critical: it forces Prefect to mark this task
    # as FAILED if any dbt model errors out or any dbt test fails.
    runner = PrefectDbtRunner(
        settings=settings,
        raise_on_failure=True,
    )

    # Execute `dbt deps` first if you use packages (like dbt_utils),
    # then run `dbt build` against the specified target (dev or prod).
    logger.info("Running dbt build (seeds -> staging -> marts -> tests)...")
    runner.invoke(["build", "--target", target])
    logger.info("dbt build completed with all models and tests passing.")


@flow(
    name="Animal Shelter ELT Pipeline",
    description=(
        "End-to-end orchestrated ELT pipeline ingesting animal shelter API data "
        "into BigQuery and transforming it into a tested Kimball star schema."
    ),
    # log_prints=True captures any standard print() statements inside your
    # existing Python ingestion script and routes them to the Prefect UI logs.
    log_prints=True,
)
def animal_shelter_elt_flow(
    dbt_project_dir: str = "shelter_warehouse",  # where to find the dbt_project.yml file & mo
    dbt_profiles_dir: str = "~/.dbt",  # where to find the profiles.yml file
    dbt_target: str = "dev",  # which target environment to use (e.g., dev or prod)
) -> None:
    """
    Main orchestration workflow (the DAG controller).

    Why parameterize directories and target?
        Default values work cleanly when running locally on your Mac, while
        Docker or production deployments can override `dbt_target="prod"` or
        point to container paths without changing a single line of code.
    """
    logger = get_run_logger()
    logger.info("=== Starting Animal Shelter ELT Pipeline ===")

    # Step 1: Extract & Load raw data into BigQuery.
    # Because Step 2 is called sequentially after Step 1, Prefect enforces an
    # implicit dependency: Step 2 will NEVER start unless Step 1 succeeds.
    extract_and_load_bronze()

    # Step 2: Transform & Validate data in BigQuery using dbt Core.
    transform_and_test_dbt(
        project_dir=Path(dbt_project_dir).resolve(),
        profiles_dir=Path(dbt_profiles_dir).expanduser().resolve(),
        target=dbt_target,
    )

    logger.info("=== Animal Shelter ELT Pipeline Finished Successfully ===")


if __name__ == "__main__":
    # Allows running the entire orchestrated flow directly from the terminal
    # via `python flow.py` for quick local testing and debugging.
    animal_shelter_elt_flow()
