with source as (
    select * from {{ source('demo_tea_e_shop', 'identifies') }}
),

renamed as (
    select
        -- ids
        id                              as event_id,
        anonymous_id,
        user_id,

        -- user traits
        context_traits_email            as email,
        context_traits_first_name       as first_name,
        context_traits_last_name        as last_name,
        context_traits_name             as full_name,

        -- timestamps
        timestamp                       as event_timestamp,
        received_at,
        sent_at,

        -- session / channel
        channel,
        context_session_id              as session_id,
        context_session_start           as is_session_start,

        -- page context
        context_page_url                as page_url,
        context_page_path               as page_path,

        -- device / browser
        context_user_agent              as user_agent,
        context_locale                  as locale,
        context_timezone                as timezone,
        context_ip                      as ip_address

    from source
)

select * from renamed
