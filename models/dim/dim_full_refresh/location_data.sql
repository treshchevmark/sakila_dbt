{{
  config(
    materialized = 'ephemeral',
    )
}}

select 
address.address_id,
address.address,
city.city_id,
city.city,
country.country_id,
country.country
from {{ source('stg', 'address') }} as address 
left join {{ source('stg', 'city') }} as city on address.city_id = city.city_id
left join {{ source('stg', 'country') }} as country on country.country_id = city.country_id