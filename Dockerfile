# ============================================================================
# Dockerfile — Portable Linux Runtime for Animal Shelter ELT Pipeline
# ============================================================================

# 1. BASE IMAGE
# WHY `python:3.11-slim`?
# - `3.11` is rock-solid with Prefect 3, dbt-bigquery, and Google Cloud SDKs.
# - `-slim` strips out unnecessary GUI/desktop packages from Debian Linux,
#   shrinking the image from ~1 GB down to ~150 MB with fewer security risks.
FROM python:3.11-slim

# 2. PYTHON RUNTIME ENVIRONMENT VARIABLES
# WHY `PYTHONDONTWRITEBYTECODE=1`? Stops Python from writing `.pyc` cache files.
# WHY `PYTHONUNBUFFERED=1`? Forces Python and Prefect logs to stream straight to
#   your terminal immediately instead of getting stuck in a buffer if a crash occurs.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 3. SYSTEM DEPENDENCIES
# WHY `git`? `dbt deps` uses git under the hood to download dbt packages
#   (like dbt_utils) from GitHub.
# WHY `rm -rf /var/lib/apt/lists/*`? Deletes temporary apt cache files in the
#   same layer so installer clutter doesn't bloat the final image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# 4. WORKING DIRECTORY
# Creates `/app` inside the Linux container and sets it as the default directory.
WORKDIR /app

# 5. INSTALL PYTHON DEPENDENCIES (LAYER CACHING OPTIMIZATION)
# WHY copy `requirements.txt` BEFORE copying `flow.py`, `src/`, and `shelter_warehouse/`?
# Docker caches every step as a layer. Because `requirements.txt` changes rarely
# compared to your SQL or Python code, Docker reuses this cached `pip install`
# layer on future rebuilds—cutting rebuild times from ~2 minutes to ~2 seconds.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. COPY PIPELINE CODE
# Copies `flow.py`, `src/`, and `shelter_warehouse/` into `/app`.
# Anything listed in `.dockerignore` is automatically skipped.
COPY . .

# 7. PRE-INSTALL DBT PACKAGES (If packages.yml exists)
# WHY run `dbt deps` during image build? Baking dbt packages into the image
# ensures your pipeline won't fail if GitHub has a hiccup during a scheduled run.
RUN if [ -f shelter_warehouse/packages.yml ]; then \
      dbt deps --project-dir shelter_warehouse; \
    fi

# 8. DEFAULT STARTUP COMMAND
# Runs your Prefect orchestrator automatically whenever the container starts.
CMD ["python", "flow.py"]
