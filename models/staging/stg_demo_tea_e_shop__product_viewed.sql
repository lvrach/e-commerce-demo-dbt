with source as (
    select * from {{ source('demo_tea_e_shop', 'product_viewed') }}
),

renamed as (
    select
        -- ids
        id                              as event_id,
        anonymous_id,
        user_id,

        -- product attributes
        product_id,
        name                            as product_name,
        category,
        brand,
        sku,
        price,
        currency,
        variant,
        url                             as product_url,
        image_url,

        -- timestamps
        timestamp                       as event_timestamp,
        received_at,
        sent_at,

        -- session / channel
        channel,
        context_session_id              as session_id,

        -- page context
        context_page_url                as page_url,
        context_page_path               as page_path,

        -- user context
        context_traits_email            as user_email,

        -- campaign
        context_campaign_source         as utm_source,
        context_campaign_medium         as utm_medium,
        context_campaign_name           as utm_campaign,

        -- device / browser
        context_user_agent              as user_agent,
        context_locale                  as locale,
        context_ip                      as ip_address

    from source
)

select * from renamed
