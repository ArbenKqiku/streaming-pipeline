{{
    config(
        materialized='table'
        )
}}

select
    company_name,
    company_number,
    -- company_status,
    concat("test_", current_timestamp()) as company_status,
    date_of_creation,
    postal_code,
    stream.published_at
from
    {{ source("staging", "company_house_stream") }} stream
join
    {{ source("production", "get_last_timestamp") }} last_timestamp
    on stream.published_at > last_timestamp.published_at