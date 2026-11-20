# Animal Shelter Operations & Stay Analytics ELT

An end-to-end analytics engineering pipeline built with **dbt Core**, **Google BigQuery**, and **SQL** to transform ~80,000 raw animal shelter intake and outcome records into a governed, tested dimensional warehouse.

---

## Project Overview

Municipal animal shelter datasets often arrive as wide, flat exports containing duplicate stay records, inconsistent string formatting, mixed-breed text fields, and open stays without outcomes. This project applies modern analytics engineering best practices—medallion architecture, surrogate key generation, window-function event sequencing, and automated data quality testing—to turn raw operational logs into reliable analytical models.

### Key Engineering Highlights
* **Cloud Data Warehousing (BigQuery):** Modeled directly on Google BigQuery using native SQL transformations, date partitioning, and clustering for query efficiency.
* **Medallion Architecture (`dbt Core`):**
  * **Staging (`models/staging`):** 1-to-1 source standardization, whitespace trimming, type casting (`FLOAT64` visit counts to `INT64`, timestamps to dates), and deterministic MD5 surrogate primary key (`stay_id`) generation.
  * **Intermediate (`models/intermediate`):** *(In Progress)* Breed parsing, spay/neuter status extraction, and `ROW_NUMBER()` / `LAG()` window functions to sequence repeat animal visits and calculate return rates.
  * **Marts (`models/marts`):** *(Planned)* Kimball-style dimensional tables (`dim_animals`, `fct_shelter_stays`) designed for self-serve operational reporting.
* **Data Quality & Governance:** Automated `unique`, `not_null`, and business-rule tests defined in YAML alongside model and column-level lineage documentation.

---

## Repository Structure

```text
shelter-analytics-elt/
├── .gitignore                 # Excludes virtual environments, dbt artifacts, and credentials
├── README.md                  # Project overview, architecture, and setup guide
└── shelter_warehouse/         # dbt Core project directory
    ├── dbt_project.yml        # Project configuration and materialization rules
    ├── models/
    │   ├── staging/
    │   │   ├── sources.yml            # Raw BigQuery source definitions & source tests
    │   │   ├── stg_shelter_stays.sql  # Staging cleanup and surrogate key generation
    │   │   └── stg_shelter_stays.yml  # Staging column documentation & data tests
    │   ├── intermediate/              # Business logic & repeat-visit window functions
    │   └── marts/                     # Dimensional facts and dimensions
    ├── tests/                         # Custom singular SQL data quality tests
    └── macros/                        # Reusable Jinja SQL macros
```

---

## Tech Stack

* **Data Warehouse:** Google BigQuery
* **Transformation & Testing:** dbt Core (`dbt-bigquery`)
* **Languages:** SQL (GoogleSQL), YAML, Jinja
* **Local Development:** macOS, VS Code, Python `.venv`, Google Cloud CLI, Git/GitHub

---

## Data Quality Findings Log

* **Duplicate Stay Records (`unique_stg_shelter_stays_stay_id`):** Initial staging validation across ~80,000 raw records flagged 26 duplicate surrogate keys (`animal_id` + `intake_datetime` + `intake_visit_number`), demonstrating the necessity of automated uniqueness assertions before building downstream metrics.

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

3. **Verify connection and run models/tests:**
   ```bash
   cd shelter_warehouse
   dbt debug
   dbt build
   ```
