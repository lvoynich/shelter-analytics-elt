# Animal Shelter Operations & Stay Analytics ELT

An end-to-end analytics engineering pipeline built with **Python**, **dbt Core**, **Google BigQuery**, and **GitHub Actions** to ingest, clean, enrich, and model ~80,000 animal shelter records into a governed Kimball star schema.

---

## Project Overview

This project combines **Python API ingestion** with a **three-tier medallion warehouse in dbt Core and BigQuery** to address duplicate stay records, inconsistent string formatting, and open stays. It features incremental `MERGE` materialization, window-function visit sequencing, a `dbt seed` entity-resolution crosswalk achieving a **99.5% canine match rate**, custom singular data tests, and automated CI/CD linting. For the complete documentation, architecture diagrams, data quality findings, repository layout, and setup instructions, please see the full project details in the main repository.
