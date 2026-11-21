with source as (

    select * from {{ source('animal_shelter', 'raw_dog_breeds') }}

),

parsed as (

    select
        breed_id,
        trim(breed_name) as breed_name,
        lower(trim(breed_name)) as breed_name_clean,
        coalesce(nullif(trim(breed_group), ''), 'Unknown') as breed_group,
        trim(bred_for) as bred_for,
        trim(temperament) as temperament,
        trim(origin) as origin,

        (
            select min(safe_cast(w as int64))
            from unnest(regexp_extract_all(weight_imperial, r'\d+')) as w
        ) as min_weight_lbs,

        (
            select max(safe_cast(w as int64))
            from unnest(regexp_extract_all(weight_imperial, r'\d+')) as w
        ) as max_weight_lbs,

        safe_cast(regexp_extract(life_span, r'^(\d+)') as int64) as min_life_span_years,
        safe_cast(regexp_extract(life_span, r'-\s*(\d+)') as int64) as max_life_span_years,

        ingested_at

    from source

),

final as (

    select
        *,
        case
            when max_weight_lbs is null then 'Unknown'
            when max_weight_lbs <= 12 then 'Toy'
            when max_weight_lbs <= 25 then 'Small'
            when max_weight_lbs <= 60 then 'Medium'
            when max_weight_lbs <= 100 then 'Large'
            else 'Giant'
        end as size_category
    from parsed
    qualify row_number() over (
        partition by breed_name_clean
        order by
            case when max_weight_lbs is not null then 0 else 1 end,
            breed_id asc
    ) = 1

)

select * from final
