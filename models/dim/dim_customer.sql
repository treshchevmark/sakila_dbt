{{
  config(
    unique_key = 'customer_id',
    post_hook="{{ manual_refresh(this) }}"
    )
}}
with refresh_date as (
    select 
    from_date
    from {{ source('stg', 'z_refresh_from') }}
    where table_name = '{{this}}' and to_refresh = 1
)
select 
city.city as city_name,
cst.first_name || ' ' || cst.last_name as full_name,
right(cst.email, length(cst.email) - position('@' in cst.email)) as domain,
case when cst.activebool then 'yes' else 'no' end as active_desc,
cst.*
from {{ source('stg', 'cust') }} as cst
left join {{ source('stg', 'address') }} as address on address.address_id = cst.address_id
left join {{ source('stg', 'city') }} as city on address.city_id = city.city_id
{% if is_incremental() %}
  where cst.last_update > coalesce(
                                      (select from_date from refresh_date), 
                                      (select max(last_update) from {{ this }}), 
                                      '1900-01-01'
)
{% endif %}