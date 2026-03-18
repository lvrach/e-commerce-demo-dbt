with source as (
    select * from {{ source('demo_tea_e_shop', 'tracks') }}
),

renamed as (
    select
        -- ids
        id                              as event_id,
        anonymous_id,
        user_id,

        -- event
        event                           as event_name,
        event_text,

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
        context_page_title              as page_title,
        context_page_referrer           as page_referrer,
        context_page_referring_domain   as referring_domain,
        context_page_search             as page_search,

        -- user context
        context_traits_email            as user_email,
        context_traits_first_name       as user_first_name,
        context_traits_last_name        as user_last_name,

        -- campaign
        context_campaign_source         as utm_source,
        context_campaign_medium         as utm_medium,
        context_campaign_name           as utm_campaign,

        -- device / browser
        context_user_agent              as user_agent,
        context_locale                  as locale,
        context_timezone                as timezone,
        context_ip                      as ip_address,
        context_screen_width            as screen_width,
        context_screen_height           as screen_height

    from source
)

select * from renamed
