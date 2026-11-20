{# =============================================================================
FILE:    models/staging/stg_shelter_stays.sql
PURPOSE: 1-to-1 staging cleanup layer over the raw shelter table.
WHAT IT DOES:
  1. Generates a deterministic surrogate primary key (stay_id) using MD5.
  2. Standardizes column names (e.g., intake_number -> intake_visit_number).
  3. Casts float visit numbers to integers and timestamps to dates.
  4. Trims extra whitespace from text fields and drops redundant helper columns.
MATERIALIZATION: Built as a BigQuery VIEW (configured in dbt_project.yml).
============================================================================= #}

with source as (

    -- Pull raw records from the registered BigQuery source
    select * from {{ source('animal_shelter', 'intakes_outcomes') }}

),

cleaned as (

    select
        -- Surrogate primary key uniquely identifying each shelter stay
        to_hex(
            md5(
                concat(
                    coalesce(trim(animal_id_intake), trim(animal_id_outcome), 'unknown'),
                    '-',
                    cast(intake_datetime as string),
                    '-',
                    coalesce(cast(cast(intake_number as int64) as string), '0')
                )
            )
        ) as stay_id,

        -- Animal identifiers & demographics
        coalesce(trim(animal_id_intake), trim(animal_id_outcome)) as animal_id,
        trim(animal_id_intake) as animal_id_intake,
        trim(animal_id_outcome) as animal_id_outcome,
        trim(animal_type) as animal_type,
        trim(breed) as breed,
        trim(color) as color,
        date(date_of_birth) as date_of_birth,

        -- Intake event details
        intake_datetime,
        cast(intake_number as int64) as intake_visit_number,
        trim(intake_type) as intake_type,
        trim(intake_condition) as intake_condition,
        trim(found_location) as found_location,
        trim(sex_upon_intake) as sex_upon_intake,
        trim(age_upon_intake) as age_upon_intake,
        age_upon_intake_days,
        age_upon_intake_years,
        trim(age_upon_intake_age_group) as age_upon_intake_age_group,

        -- Outcome event details (null if the animal is still in shelter care)
        outcome_datetime,
        cast(outcome_number as int64) as outcome_visit_number,
        trim(outcome_type) as outcome_type,
        trim(outcome_subtype) as outcome_subtype,
        trim(sex_upon_outcome) as sex_upon_outcome,
        trim(age_upon_outcome) as age_upon_outcome,
        age_upon_outcome_days,
        age_upon_outcome_years,
        trim(age_upon_outcome_age_group) as age_upon_outcome_age_group,

        -- Stay duration metrics
        trim(time_in_shelter) as time_in_shelter_text,
        round(time_in_shelter_days, 2) as time_in_shelter_days

    from source

)

select * from cleaned
