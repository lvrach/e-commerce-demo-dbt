/*
  Order aggregates per user, with anonymous orders stitched to known
  users via int_user_identity.
*/
with orders as (
    select
        coalesce(o.user_id, id.user_id)     as user_id,
        o.order_id,
        o.order_total,
        o.order_discount,
        o.coupon,
        o.currency,
        o.checkout_flow,
        o.event_timestamp
    from {{ ref('stg_demo_tea_e_shop__order_completed') }} o
    left join {{ ref('int_user_identity') }} id
        on o.anonymous_id = id.anonymous_id
),

aggregated as (
    select
        user_id,
        count(*)                                            as total_orders,
        count(distinct order_id)                            as unique_orders,
        sum(order_total)                                    as lifetime_value,
        avg(order_total)                                    as avg_order_value,
        max(order_total)                                    as max_order_value,
        min(event_timestamp)                                as first_order_at,
        max(event_timestamp)                                as last_order_at,
        count(coupon) filter (where coupon is not null)     as orders_with_coupon,
        sum(order_discount)                                 as total_discount,
        extract(day from now() - max(event_timestamp))      as days_since_last_order,
        mode() within group (order by checkout_flow)        as preferred_checkout_flow,
        mode() within group (order by currency)             as preferred_currency
    from orders
    where user_id is not null
    group by 1
)

select * from aggregated
