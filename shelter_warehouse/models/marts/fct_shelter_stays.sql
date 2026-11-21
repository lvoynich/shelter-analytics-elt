{# =============================================================================
FILE:    models/marts/fct_shelter_stays.sql
PURPOSE: Gold Kimball fact table capturing every individual shelter stay event.
WHAT IT DOES:
  1. Materializes incrementally using a BigQuery MERGE strategy on stay_id.
  2. On incremental runs, processes only newly arrived intakes and previously
     open stays that may have recently received an outcome.
  3. Partitions by intake_date (monthly) and clusters by species and outcome
     to optimize BigQuery scan costs and query performance.
MATERIALIZATION: Incremental BigQuery TABLE (partitioned & clustered).
============================================================================= #}

{{
    config(
        materialized='incremental',
        unique_key='stay_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        partition_by={
            'field': 'intake_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=['animal_type', 'outcome_type']
    )
}}

with stays as (

    select * from {{ ref('int_animal_stays') }}

    {% if is_incremental() %}
        -- Filter to new stays since the high-water mark, plus any previously open stays
        -- so newly recorded outcomes update existing rows via MERGE
        where intake_datetime >= (select max(intake_datetime) from {{ this }})
           or stay_id in (select stay_id from {{ this }} where is_open_stay = true)
    {% endif %}

)

select
    -- Primary & foreign keys
    stay_id,
    animal_id,

    -- Denormalized filter attributes for fast partition/cluster pruning
    animal_type,
    intake_date,
    outcome_date,

    -- Intake event descriptors
    intake_datetime,
    calculated_visit_number as visit_number,
    is_repeat_intake,
    intake_type,
    intake_condition,
    found_location,
    intake_reproductive_status,
    age_upon_intake_days,
    age_upon_intake_years,
    age_upon_intake_age_group,

    -- Outcome event descriptors
    outcome_datetime,
    outcome_type,
    outcome_subtype,
    outcome_reproductive_status,
    was_altered_during_stay,
    age_upon_outcome_days,
    age_upon_outcome_years,
    age_upon_outcome_age_group,

    -- Operational KPIs & flags
    is_open_stay,
    time_in_shelter_days,
    days_since_previous_outcome,
    is_30_day_return

from stays
