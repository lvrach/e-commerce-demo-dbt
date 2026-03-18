with source as (
    select * from {{ source('demo_tea_e_shop', 'order_completed') }}
),

renamed as (
    select
        -- ids
        id                              as event_id,
        anonymous_id,
        user_id,
        order_id,

        -- order financials
        total                           as order_total,
        subtotal                        as order_subtotal,
        tax                             as order_tax,
        shipping                        as order_shipping,
        discount                        as order_discount,
        coupon,
        currency,

        -- products (JSON array serialised as text by RudderStack)
        products,

        -- checkout context
        checkout_flow,

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

        -- device / browser
        context_user_agent              as user_agent,
        context_locale                  as locale,
        context_ip                      as ip_address

    from source
)

select * from renamed
