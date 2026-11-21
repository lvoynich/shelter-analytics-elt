{# =============================================================================
FILE:    models/marts/dim_animals.sql
PURPOSE: Gold Kimball dimension table representing one unique animal per row,
         enriched with external breed metadata from The Dog API via a seed crosswalk.
WHAT IT DOES:
  1. Deduplicates multi-visit animals to a single canonical profile (grain = animal_id).
  2. Retains the most up-to-date demographic & reproductive attributes.
  3. Resolves municipal shelter breed shorthand using the breed_aliases seed.
  4. Left-joins standardized breed group, size category, weight range, and
     temperament from stg_dog_breeds for canine records.
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

aliases as (

    select * from {{ ref('breed_aliases') }}

),

breeds as (

    select * from {{ ref('stg_dog_breeds') }}

),

animal_profiles as (

    select
        stays.animal_id,
        stays.animal_type,
        stays.raw_breed,
        stays.primary_breed,
        stays.secondary_breed,
        stays.is_mixed_breed,
        stays.color,
        stays.date_of_birth,
        stays.biological_sex,
        coalesce(stays.outcome_reproductive_status, stays.intake_reproductive_status) as current_reproductive_status,

        -- Enriched breed attributes from The Dog API (populated for matched dogs)
        breeds.breed_group,
        coalesce(nullif(breeds.size_category, 'Unknown'), aliases.fallback_size_category, 'Unknown') as size_category,

        breeds.min_weight_lbs,
        breeds.max_weight_lbs,
        breeds.temperament as breed_temperament,

        -- Lifetime summary metrics computed across all stays for each animal
        count(*) over (partition by stays.animal_id) as total_shelter_stays,
        min(stays.intake_date) over (partition by stays.animal_id) as first_intake_date,
        max(stays.intake_date) over (partition by stays.animal_id) as latest_intake_date,
        round(sum(stays.time_in_shelter_days) over (partition by stays.animal_id), 2) as lifetime_days_in_shelter,
        stays.outcome_type as latest_outcome_type,
        stays.is_open_stay as is_currently_in_shelter

    from stays
    left join aliases
        on
            lower(trim(stays.primary_breed)) = aliases.shelter_breed
            and stays.animal_type = 'Dog'
    left join breeds
        on
            coalesce(aliases.api_breed_name, lower(trim(stays.primary_breed))) = breeds.breed_name_clean
            and stays.animal_type = 'Dog'

    -- Keep the most recent stay record per animal for canonical profile attributes
    qualify row_number() over (
        partition by stays.animal_id
        order by stays.intake_datetime desc, stays.stay_id desc
    ) = 1

)

select * from animal_profiles
