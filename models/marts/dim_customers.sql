/*
  Customer 360 — one row per known user.

  Joins identity, traits, order history, and behavioural engagement into
  a single mart table. Adds a derived customer_segment based on recency
  and purchase frequency.

  Materialized as a table so BI tools always query a pre-built snapshot.
*/
with traits as (
    select * from {{ ref('int_user_traits') }}
),

orders as (
    select * from {{ ref('int_user_orders') }}
),

engagement as (
    select * from {{ ref('int_user_engagement') }}
),

joined as (
    select
        -- identity
        t.user_id,
        t.email,
        t.first_name,
        t.last_name,
        t.full_name,
        t.locale,
        t.timezone,
        t.traits_updated_at,

        -- lifecycle timestamps
        e.first_seen_at,
        e.last_seen_at,
        o.first_order_at,
        o.last_order_at,

        -- order metrics
        coalesce(o.total_orders, 0)                 as total_orders,
        coalesce(o.unique_orders, 0)                as unique_orders,
        coalesce(o.lifetime_value, 0)               as lifetime_value,
        o.avg_order_value,
        o.max_order_value,
        o.orders_with_coupon,
        coalesce(o.total_discount, 0)               as total_discount,
        o.days_since_last_order,
        o.preferred_checkout_flow,
        o.preferred_currency,

        -- engagement metrics
        coalesce(e.total_sessions, 0)               as total_sessions,
        coalesce(e.total_page_views, 0)             as total_page_views,
        coalesce(e.total_product_views, 0)          as total_product_views,
        coalesce(e.total_cart_adds, 0)              as total_cart_adds,
        coalesce(e.total_searches, 0)               as total_searches,
        coalesce(e.total_checkouts_started, 0)      as total_checkouts_started,
        coalesce(e.total_wishlist_saves, 0)         as total_wishlist_saves,
        e.avg_pages_per_session,
        e.add_to_cart_rate,

        -- derived flags
        (coalesce(o.total_orders, 0) > 1)           as is_repeat_buyer,
        (coalesce(o.orders_with_coupon, 0) > 0)     as is_coupon_user,

        -- customer segment (recency x frequency)
        case
            when coalesce(o.total_orders, 0) = 0
                then 'prospect'
            when o.days_since_last_order <= 30
                and o.total_orders = 1
                then 'new_customer'
            when o.days_since_last_order <= 30
                and o.total_orders > 1
                then 'repeat_customer'
            when o.days_since_last_order between 31 and 90
                then 'at_risk'
            when o.days_since_last_order > 90
                then 'churned'
            else 'unknown'
        end                                         as customer_segment

    from traits t
    left join orders     o on t.user_id = o.user_id
    left join engagement e on t.user_id = e.user_id
)

select * from joined
