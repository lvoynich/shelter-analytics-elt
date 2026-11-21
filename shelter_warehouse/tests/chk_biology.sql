with visit_history as (

    select
        stay_id,
        animal_id,
        visit_number,
        intake_reproductive_status,
        outcome_reproductive_status,
        lag(outcome_reproductive_status) over (
            partition by animal_id
            order by visit_number
        ) as previous_outcome_status
    from {{ ref('fct_shelter_stays') }}

)

select
    stay_id,
    animal_id,
    visit_number,
    previous_outcome_status,
    intake_reproductive_status,
    outcome_reproductive_status
from visit_history
where
    previous_outcome_status = 'Altered'
    and (
        intake_reproductive_status = 'Intact'
        or outcome_reproductive_status = 'Intact'
    )
