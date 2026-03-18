with source as (
    select * from {{ source('demo_tea_e_shop', 'pages') }}
),

renamed as (
    select
        -- ids
        id                              as event_id,
        anonymous_id,
        user_id,

        -- page properties
        name                            as page_name,
        title                           as page_title,
        url                             as page_url,
        path                            as page_path,
        search                          as page_search,
        referrer,
        referring_domain,
        initial_referrer,
        initial_referring_domain,
        tab_url,
        category,

        -- timestamps
        timestamp                       as event_timestamp,
        received_at,
        sent_at,

        -- session / channel
        channel,
        context_session_id              as session_id,
        context_session_start           as is_session_start,

        -- user context
        context_traits_email            as user_email,

        -- campaign
        context_campaign_source         as utm_source,
        context_campaign_medium         as utm_medium,
        context_campaign_name           as utm_campaign,

        -- device / browser
        context_user_agent              as user_agent,
        context_locale                  as locale,
        context_timezone                as timezone,
        context_ip                      as ip_address

    from source
)

select * from renamed
