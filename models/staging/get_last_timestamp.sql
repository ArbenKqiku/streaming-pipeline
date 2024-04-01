{{
    config(
        materialized='table'
    )
}}

select
    published_at
from
    {{ source('development', 'snapshot_streamed_data')}}
order by
    published_at desc
limit
    1