{{
  config(
    indexes= [{'columns': ['create_date']}],
    post_hook="insert into {{ this }} (customer_id, first_name) values (-1,'NA')"
    ) 
}}

select 
loc.address as address_name,
loc.city as city_name,
loc.country as country_name,
cst.first_name || ' ' || cst.last_name as full_name,
right(cst.email, length(cst.email) - position('@' in cst.email)) as domain,
case when cst.activebool then 'yes' else 'no' end as active_desc,
cst.*,
'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('stg', 'cust') }} as cst
left join {{ ref('location_data') }} as loc on loc.address_id = cst.address_id