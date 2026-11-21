{{ config(severity = 'warn') }}

select
    stay_id,
    animal_id,
    intake_datetime,
    outcome_datetime,
    age_upon_intake_days,
    age_upon_outcome_days
from {{ ref('fct_shelter_stays') }}
where age_upon_outcome_days < age_upon_intake_days
