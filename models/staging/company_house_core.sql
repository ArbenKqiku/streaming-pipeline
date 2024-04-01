{{
    config(
        materialized='table'
    )
}}

select
    *
from
    {{ source('staging', 'company_house_core')}}