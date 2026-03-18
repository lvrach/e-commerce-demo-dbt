/*
  Identity stitching: maps every anonymous_id to the most recently
  associated authenticated user_id.

  RudderStack fires an identify() call when a user logs in or signs up,
  linking the pre-auth anonymous_id to the user_id. We take the latest
  user_id per anonymous_id so downstream models can attribute anonymous
  activity back to known users.
*/
with id_map as (
    select
        anonymous_id,
        user_id,
        event_timestamp,
        row_number() over (
            partition by anonymous_id
            order by event_timestamp desc
        ) as rn
    from {{ ref('stg_demo_tea_e_shop__identifies') }}
    where user_id is not null
)

select
    anonymous_id,
    user_id,
    event_timestamp as identified_at
from id_map
where rn = 1
