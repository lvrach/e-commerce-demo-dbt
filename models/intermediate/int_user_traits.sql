/*
  Latest-snapshot user traits per user_id.

  identify() can be called multiple times (e.g. on each login). We take
  only the most recent call so downstream models get a single current-state
  row per user.
*/
with ranked as (
    select
        user_id,
        email,
        first_name,
        last_name,
        full_name,
        locale,
        timezone,
        event_timestamp,
        row_number() over (
            partition by user_id
            order by event_timestamp desc
        ) as rn
    from {{ ref('stg_demo_tea_e_shop__identifies') }}
    where user_id is not null
)

select
    user_id,
    email,
    first_name,
    last_name,
    full_name,
    locale,
    timezone,
    event_timestamp as traits_updated_at
from ranked
where rn = 1
