{{ config(
    materialized='view'
) }}

SELECT
    -- Primary & foreign keys
    f.stay_id,
    f.animal_id,

    -- Partition & outcome dates
    f.intake_date,
    f.outcome_date,

    -- Visit sequence & intake context
    f.visit_number,
    f.is_repeat_intake,
    f.intake_type,
    f.intake_condition,
    f.found_location,
    f.intake_reproductive_status,

    -- Stay-level age at intake
    f.age_upon_intake_days,
    f.age_upon_intake_years,
    f.age_upon_intake_age_group,

    -- Outcome details & spay/neuter conversion flag
    f.outcome_type,
    f.outcome_subtype,
    f.outcome_reproductive_status,
    f.was_altered_during_stay,

    -- Stay-level KPIs and status flags
    f.is_open_stay,
    f.time_in_shelter_days,
    f.days_since_previous_outcome,
    f.is_30_day_return,

    -- Enriched animal & breed attributes from dim_animals
    f.animal_type,
    d.primary_breed,
    d.secondary_breed,
    d.is_mixed_breed,
    d.color,
    d.biological_sex,
    d.breed_group,
    d.size_category,
    d.breed_temperament,
    d.total_shelter_stays

FROM {{ ref('fct_shelter_stays') }} AS f
LEFT JOIN {{ ref('dim_animals') }} AS d
    ON f.animal_id = d.animal_id
