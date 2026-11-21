select
    stay_id,
    animal_id,
    intake_datetime,
    outcome_datetime
from {{ ref('fct_shelter_stays') }}
where outcome_datetime < intake_datetime
