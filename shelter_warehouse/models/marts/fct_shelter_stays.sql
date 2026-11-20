{# =============================================================================
FILE:    models/marts/fct_shelter_stays.sql
PURPOSE: Gold Kimball fact table capturing every individual shelter stay event.
WHAT IT DOES:
  1. Stores foreign keys (animal_id), event timestamps, and operational metrics.
  2. Partitions by intake_date (monthly) and clusters by species and outcome
     to optimize BigQuery scan costs and query performance.
MATERIALIZATION: Built as a partitioned & clustered BigQuery TABLE.
============================================================================= #}

{{
    config(
        materialized='table',
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
