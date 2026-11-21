# Animal Shelter Operations & Stay Analytics ELT
[![Data Pipeline CI](https://://github.com/shelter-analytics-elt/actions/workflows/ci.yml/badge.svg)](https://://github.com/shelter-analytics-elt/actions/workflows/ci.yml)

An end-to-end analytics engineering pipeline built with **dbt Core**, **Google BigQuery**, and **SQL** to transform ~80,000 raw animal shelter intake and outcome records into a governed, tested Kimball star schema.

---

## Project Overview

Municipal animal shelter datasets often arrive as wide, flat exports containing duplicate stay records, inconsistent string formatting, mixed-breed text fields, and open stays without outcomes. This project applies modern analytics engineering best practices—medallion architecture, surrogate key generation, window-function event sequencing, BigQuery partitioning/clustering, and automated data quality testing—to turn raw operational logs into reliable analytical models.

### Architecture & Medallion Layers

```text
[Bronze: BigQuery Raw Source]
   └── animal_shelter.intakes_outcomes (~80k rows)
          │
          ▼
[Silver: Staging & Enrichment Views]
   ├── stg_shelter_stays   (Type casting, trimming, MD5 surrogate keys, deduplication)
   └── int_animal_stays    (Breed/sex parsing, ROW_NUMBER/LAG visit sequencing, 30-day return flags)
          │
          ▼
[Gold: Kimball Star Schema Tables]
   ├── dim_animals         (1 row per unique animal; clustered by species & primary breed)
   └── fct_shelter_stays   (1 row per shelter stay; partitioned by intake_date, clustered by species & outcome)
```
![dbt Lineage Graph](assets/lineage_dag.png)
*(Note: To view the interactive data dictionary and lineage DAG locally, run `dbt docs generate && dbt docs serve` inside `shelter_warehouse/`.)*

### Key Engineering Highlights
* **Cloud Data Warehousing (BigQuery):** Gold fact tables are configured with **monthly date partitioning** (`intake_date`) and **multi-column clustering** (`animal_type`, `outcome_type`, `primary_breed`) to minimize query scan costs and improve performance.
* **Window-Function Event Sequencing:** Uses `ROW_NUMBER()`, `COUNT(*) OVER (...)`, and `LAG()` partitioned by `animal_id` to reconstruct multi-visit animal histories, calculate days between stays, and flag 30-day returns.
* **Attribute Standardization:** Parses raw breed strings into `primary_breed`, `secondary_breed`, and `is_mixed_breed`, while separating biological sex from reproductive status to track in-shelter spay/neuter conversions (`was_altered_during_stay`).
* **Automated Data Governance (26/26 Checks Passing):** Enforces `unique`, `not_null`, `accepted_values`, and foreign-key `relationships` tests across every layer of the DAG.
* **Python API Ingestion (`src/ingest_dog_breeds.py`):** Extracts 631 canine breed records from The Dog API with exponential-backoff retries, structured logging, and explicit BigQuery schema enforcement (`WRITE_TRUNCATE`).
* **Entity Resolution via `dbt seed` (`breed_aliases.csv`):** Resolves municipal shelter breed shorthand and applies AKC size fallbacks, boosting canine breed match rates from 59.6% to **99.5%**.
* **Incremental `MERGE` & CI/CD:** Materializes `fct_shelter_stays` incrementally on `stay_id` (updating newly closed open stays) and validates every push via GitHub Actions (`ruff`, `sqlfluff`, and `dbt parse`).


---

## Data Quality Findings & Resolutions

* **Duplicate Stay Records (`unique_stg_shelter_stays_stay_id`):**
  * **Finding:** Initial staging validation uncovered 26 duplicate `stay_id` keys (`animal_id` + `intake_datetime` + `intake_visit_number`), with some records repeated up to 4 times due to upstream export join fan-out.
  * **Resolution:** Implemented `QUALIFY ROW_NUMBER()` deduplication in `stg_shelter_stays.sql` partitioned by `stay_id`, resolving all duplicates and achieving 100% primary-key uniqueness.

* **Intake Age Estimation Discrepancies (`chk_age`):**
  * **Finding:** Custom age validation (`age_upon_outcome_days < age_upon_intake_days`) flagged 152 stays where recorded outcome age was lower than intake age—primarily concentrated in sequential litter intakes (e.g., `A764890`–`A764898`) where staff estimated 5 weeks (`35` days) at intake and refined the birth date to 1 month (`30` days) at outcome.
  * **Resolution:** Configured `chk_age.sql` with `severity = 'warn'` to maintain observability over source estimation adjustments without blocking downstream mart builds.

* **Canine Breed Entity Resolution (`breed_aliases.csv`):**
  * **Finding:** Direct joining between shelter `primary_breed` strings and The Dog API left 40.4% of shelter dogs unmatched (`Unknown` size) due to municipal shorthand (e.g., `"Pit Bull"`, `"Chihuahua Shorthair"`) and missing API weights.
  * **Resolution:** Iteratively built a 37-row `dbt seed` crosswalk with fallback AKC size tiers, reducing `Unknown` dog sizes from 40.4% down to **0.5%** (a **99.5% match rate**).

---

## What This Warehouse Answers

Once built in BigQuery, the Gold star schema supports self-serve operational analysis across key shelter metrics:
* **30-Day Return Rates:** Tracking how often adopted animals return within 30 days by species, age group, or intake type.
* **Length of Stay (LOS) Drivers:** Comparing average and median shelter duration across mixed vs. purebred animals, intake conditions, and outcome categories.
* **In-Shelter Spay/Neuter Impact:** Measuring how many intact animals are altered during their stay (`was_altered_during_stay`) and how that affects adoption timelines.
* **Active Shelter Capacity:** Isolating open stays (`is_open_stay = true`) to monitor animals currently in care.

---

## Repository Structure

```text
shelter-analytics-elt/
├── .gitignore                         # Excludes virtual environments, dbt artifacts, and credentials
├── README.md                          # Project overview, architecture, and setup guide
└── shelter_warehouse/                 # dbt Core project directory
    ├── dbt_project.yml                # Project configuration and materialization rules
    └── models/
        ├── staging/
        │   ├── sources.yml            # Raw BigQuery source definitions & source tests
        │   ├── stg_shelter_stays.sql  # Staging cleanup, surrogate keys, and deduplication
        │   └── stg_shelter_stays.yml  # Staging column documentation & data tests
        ├── intermediate/
        │   ├── int_animal_stays.sql   # Breed/sex parsing & repeat-visit window functions
        │   └── int_animal_stays.yml   # Intermediate documentation & accepted_values tests
        └── marts/
            ├── dim_animals.sql        # Gold animal dimension table (clustered)
            ├── fct_shelter_stays.sql  # Gold shelter stay fact table (partitioned & clustered)
            └── marts.yml              # Star schema documentation & foreign-key relationship tests
            ├── .github/workflows/ci.yml           # Automated Ruff, SQLFluff, and dbt parse CI pipeline
├── requirements.txt                   # Python dependencies (dbt-bigquery, google-cloud-bigquery, requests)
├── src/
│   └── ingest_dog_breeds.py           # REST API ingestion script loading 631 breeds into BigQuery

```

---

### Custom Singular Tests (`shelter_warehouse/tests/`)
* **`chk_biology.sql`:** Uses `LAG()` window functions partitioned by `animal_id` to verify biological consistency across repeat visits, confirming zero animals reverted from `Altered` back to `Intact`.
* **`chk_age.sql`:** Monitors stays where outcome age is lower than intake age (`severity = 'warn'`), isolating bulk litter age-estimation adjustments.
* **`assert_outcome_after_intake.sql`:** Enforces chronological integrity by verifying no closed stay has an outcome timestamp prior to intake.

---

## Tech Stack

* **Data Warehouse:** Google BigQuery
* **Transformation & Testing:** dbt Core (`dbt-bigquery`)
* **Languages:** SQL (GoogleSQL), YAML, Jinja
* **Local Development:** macOS, VS Code, Python `.venv`, Google Cloud CLI, Git/GitHub

---

## Local Setup & Usage

1. **Clone the repository and activate the virtual environment:**
   ```bash
   git clone https://github.com
   cd shelter-analytics-elt
   python3 -m venv .venv
   source .venv/bin/activate
   pip install dbt-bigquery
   ```

2. **Authenticate with Google Cloud:**
   ```bash
   gcloud auth application-default login
   ```

3. **Verify connection, run models, and execute all 26 tests:**
   ```bash
   cd shelter_warehouse
   dbt debug
   dbt build
   ```

4. **Generate and view local lineage documentation:**
   ```bash
   dbt docs generate
   dbt docs serve
   ```
