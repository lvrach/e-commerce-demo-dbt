with source as (
    select * from {{ source('demo_tea_e_shop', 'payment_info_entered') }}
),

renamed as (
    select
        -- ids
        id                              as event_id,
        anonymous_id,
        user_id,
        order_id,

        -- payment
        step,
        step_name,
        payment_method,
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
