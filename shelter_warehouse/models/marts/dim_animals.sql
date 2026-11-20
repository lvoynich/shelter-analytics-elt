{# =============================================================================
FILE:    models/marts/dim_animals.sql
PURPOSE: Gold Kimball dimension table representing one unique animal per row.
WHAT IT DOES:
  1. Deduplicates multi-visit animals to a single canonical profile (grain = animal_id).
  2. Retains the most up-to-date demographic & reproductive attributes.
  3. Computes lifetime shelter metrics (total stays, first intake, latest outcome).
MATERIALIZATION: Built as a physical BigQuery TABLE clustered by animal_type.
============================================================================= #}

{{
    config(
        materialized='table',
        cluster_by=['animal_type', 'primary_breed']
    )
}}

with stays as (

    select * from {{ ref('int_animal_stays') }}

),

animal_profiles as (

    select
        animal_id,
        animal_type,
        raw_breed,
        primary_breed,
        secondary_breed,
        is_mixed_breed,
        color,
        date_of_birth,
        biological_sex,
        coalesce(outcome_reproductive_status, intake_reproductive_status) as current_reproductive_status,

        -- Lifetime summary metrics computed across all stays for each animal
        count(*) over (partition by animal_id) as total_shelter_stays,
        min(intake_date) over (partition by animal_id) as first_intake_date,
        max(intake_date) over (partition by animal_id) as latest_intake_date,
        round(sum(time_in_shelter_days) over (partition by animal_id), 2) as lifetime_days_in_shelter,
        outcome_type as latest_outcome_type,
        is_open_stay as is_currently_in_shelter

    from stays

    -- Keep the most recent stay record per animal for canonical profile attributes
    qualify row_number() over (
        partition by animal_id
        order by intake_datetime desc, stay_id desc
    ) = 1

)

select * from animal_profiles
