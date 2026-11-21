{# =============================================================================
FILE:    models/intermediate/int_animal_stays.sql
PURPOSE: Silver enrichment layer applying business logic and window functions
         to deduplicated shelter stays.
WHAT IT DOES:
  1. Parses messy breed strings into primary_breed, secondary_breed, and is_mixed_breed.
  2. Separates biological sex from spay/neuter status at intake and outcome.
  3. Calculates repeat-visit metrics per animal using ROW_NUMBER() and LAG().
  4. Computes length-of-stay metrics, open-stay flags, and 30-day return flags.
MATERIALIZATION: Built as a BigQuery VIEW (configured in dbt_project.yml).
============================================================================= #}

with stays as (

    select * from {{ ref('stg_shelter_stays') }}

),

parsed_attributes as (

    select
        *,

        -- Breed parsing: identify mixes and extract clean primary/secondary breeds
        coalesce(breed like '%/%' or lower(breed) like '% mix%', false) as is_mixed_breed,

        trim(
            regexp_replace(
                split(breed, '/')[safe_offset(0)],
                r'(?i)\s+mix$',
                ''
            )
        ) as primary_breed,

        trim(
            regexp_replace(
                split(breed, '/')[safe_offset(1)],
                r'(?i)\s+mix$',
                ''
            )
        ) as secondary_breed,

        -- Biological sex extraction
        case
            when
                lower(coalesce(sex_upon_outcome, sex_upon_intake)) like '%male%'
                and lower(coalesce(sex_upon_outcome, sex_upon_intake)) not like '%female%' then 'Male'
            when lower(coalesce(sex_upon_outcome, sex_upon_intake)) like '%female%' then 'Female'
            else 'Unknown'
        end as biological_sex,

        -- Spay/Neuter status at intake
        case
            when lower(sex_upon_intake) like 'neutered%' or lower(sex_upon_intake) like 'spayed%' then 'Altered'
            when lower(sex_upon_intake) like 'intact%' then 'Intact'
            else 'Unknown'
        end as intake_reproductive_status,

        -- Spay/Neuter status at outcome
        case
            when lower(sex_upon_outcome) like 'neutered%' or lower(sex_upon_outcome) like 'spayed%' then 'Altered'
            when lower(sex_upon_outcome) like 'intact%' then 'Intact'
            else 'Unknown'
        end as outcome_reproductive_status

    from stays

),

sequenced_stays as (

    select
        *,

        -- Flag whether the animal was spayed/neutered during this shelter stay
        coalesce(
            intake_reproductive_status = 'Intact'
            and outcome_reproductive_status = 'Altered', false
        ) as was_altered_during_stay,

        -- Window functions across an animal's full shelter history
        row_number() over (
            partition by animal_id
            order by intake_datetime asc, stay_id asc
        ) as calculated_visit_number,

        count(*) over (
            partition by animal_id
        ) as total_stays_for_animal,

        lag(outcome_datetime) over (
            partition by animal_id
            order by intake_datetime asc, stay_id asc
        ) as previous_outcome_datetime,

        -- Open stay indicator (no outcome recorded yet)
        coalesce(outcome_datetime is null, false) as is_open_stay

    from parsed_attributes

),

final as (

    select
        -- Keys
        stay_id,
        animal_id,

        -- Animal attributes
        animal_type,
        breed as raw_breed,
        primary_breed,
        secondary_breed,
        is_mixed_breed,
        color,
        date_of_birth,
        biological_sex,

        -- Intake details
        intake_datetime,
        date(intake_datetime) as intake_date,
        intake_visit_number,
        calculated_visit_number,
        total_stays_for_animal,
        calculated_visit_number > 1 as is_repeat_intake,
        intake_type,
        intake_condition,
        found_location,
        intake_reproductive_status,
        age_upon_intake_days,
        age_upon_intake_years,
        age_upon_intake_age_group,

        -- Outcome details
        outcome_datetime,
        date(outcome_datetime) as outcome_date,
        outcome_visit_number,
        outcome_type,
        outcome_subtype,
        outcome_reproductive_status,
        was_altered_during_stay,
        age_upon_outcome_days,
        age_upon_outcome_years,
        age_upon_outcome_age_group,

        -- Duration & return metrics
        is_open_stay,
        time_in_shelter_days,
        previous_outcome_datetime,
        timestamp_diff(intake_datetime, previous_outcome_datetime, day) as days_since_previous_outcome,
        coalesce(
            previous_outcome_datetime is not null
            and timestamp_diff(intake_datetime, previous_outcome_datetime, day) between 0 and 30, false
        ) as is_30_day_return

    from sequenced_stays

)

select * from final
