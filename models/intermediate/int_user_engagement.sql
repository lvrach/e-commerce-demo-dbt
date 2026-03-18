/*
  Behavioural engagement metrics per user, stitching anonymous activity
  to known users via int_user_identity.

  Covers: sessions, page views, product views, cart adds,
  searches, checkouts started, wishlist saves.
*/

-- resolve user for every track event
with tracked as (
    select
        coalesce(t.user_id, id.user_id)     as user_id,
        t.anonymous_id,
        t.session_id,
        t.event_timestamp
    from {{ ref('stg_demo_tea_e_shop__tracks') }} t
    left join {{ ref('int_user_identity') }} id
        on t.anonymous_id = id.anonymous_id
),

page_views as (
    select
        coalesce(p.user_id, id.user_id)     as user_id,
        p.session_id,
        p.event_timestamp
    from {{ ref('stg_demo_tea_e_shop__pages') }} p
    left join {{ ref('int_user_identity') }} id
        on p.anonymous_id = id.anonymous_id
),

product_views as (
    select coalesce(pv.user_id, id.user_id) as user_id
    from {{ ref('stg_demo_tea_e_shop__product_viewed') }} pv
    left join {{ ref('int_user_identity') }} id
        on pv.anonymous_id = id.anonymous_id
),

cart_adds as (
    select coalesce(pa.user_id, id.user_id) as user_id
    from {{ ref('stg_demo_tea_e_shop__product_added') }} pa
    left join {{ ref('int_user_identity') }} id
        on pa.anonymous_id = id.anonymous_id
),

searches as (
    select coalesce(id.user_id, null) as user_id
    from {{ ref('stg_demo_tea_e_shop__products_searched') }} ps
    left join {{ ref('int_user_identity') }} id
        on ps.anonymous_id = id.anonymous_id
),

checkouts as (
    select coalesce(cs.user_id, id.user_id) as user_id
    from {{ ref('stg_demo_tea_e_shop__checkout_started') }} cs
    left join {{ ref('int_user_identity') }} id
        on cs.anonymous_id = id.anonymous_id
),

wishlist as (
    select coalesce(wl.user_id, id.user_id) as user_id
    from {{ ref('stg_demo_tea_e_shop__product_added_to_wishlist') }} wl
    left join {{ ref('int_user_identity') }} id
        on wl.anonymous_id = id.anonymous_id
),

agg_sessions as (
    select
        user_id,
        count(distinct session_id)          as total_sessions,
        count(*)                            as total_events,
        min(event_timestamp)                as first_seen_at,
        max(event_timestamp)                as last_seen_at
    from tracked
    where user_id is not null
    group by 1
),

agg_pages as (
    select
        user_id,
        count(*)                            as total_page_views,
        count(distinct session_id)          as sessions_with_page_view
    from page_views
    where user_id is not null
    group by 1
),

agg_product_views as (
    select user_id, count(*) as total_product_views
    from product_views where user_id is not null group by 1
),

agg_cart_adds as (
    select user_id, count(*) as total_cart_adds
    from cart_adds where user_id is not null group by 1
),

agg_searches as (
    select user_id, count(*) as total_searches
    from searches where user_id is not null group by 1
),

agg_checkouts as (
    select user_id, count(*) as total_checkouts_started
    from checkouts where user_id is not null group by 1
),

agg_wishlist as (
    select user_id, count(*) as total_wishlist_saves
    from wishlist where user_id is not null group by 1
)

select
    s.user_id,
    s.total_sessions,
    s.total_events,
    s.first_seen_at,
    s.last_seen_at,
    coalesce(pv_pg.total_page_views, 0)             as total_page_views,
    coalesce(pv.total_product_views, 0)             as total_product_views,
    coalesce(ca.total_cart_adds, 0)                 as total_cart_adds,
    coalesce(sr.total_searches, 0)                  as total_searches,
    coalesce(ch.total_checkouts_started, 0)         as total_checkouts_started,
    coalesce(wl.total_wishlist_saves, 0)            as total_wishlist_saves,
    case
        when s.total_sessions > 0
        then round(pv_pg.total_page_views::numeric / s.total_sessions, 1)
    end                                             as avg_pages_per_session,
    case
        when coalesce(pv.total_product_views, 0) > 0
        then round(coalesce(ca.total_cart_adds, 0)::numeric / pv.total_product_views, 3)
    end                                             as add_to_cart_rate
from agg_sessions s
left join agg_pages          pv_pg on s.user_id = pv_pg.user_id
left join agg_product_views  pv    on s.user_id = pv.user_id
left join agg_cart_adds      ca    on s.user_id = ca.user_id
left join agg_searches       sr    on s.user_id = sr.user_id
left join agg_checkouts      ch    on s.user_id = ch.user_id
left join agg_wishlist       wl    on s.user_id = wl.user_id
