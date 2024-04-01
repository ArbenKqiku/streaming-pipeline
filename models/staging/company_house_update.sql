{{
    config(
        materialized='incremental',
        unique_key='company_number'
    )
}}

with ranked_data as (
select
    company_name,
    company_number,
    company_status,
    date(date_of_creation) as date_of_creation,
    postal_code,
    date(date_trunc(date_of_creation, month)) as month_of_creation,
    -- sometimes the same company is published more than once. With this, we ensure that we take only the last value
    row_number() over (partition by company_number order by published_at desc) as row_num
from
    {{source("development", "snapshot_streamed_data")}}
)

select
    company_name,
    company_number,
    company_status,
    date_of_creation,
    postal_code,
    month_of_creation,
from
    ranked_data
where
    row_num = 1