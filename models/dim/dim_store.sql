{{
  config(
    post_hook="insert into {{this}} (store_id) values (-1)"
    )
}}

select 
store.*,
loc.address as address_name,
loc.city as city_name,
loc.country as country_name,
staff.first_name,
staff.last_name,
'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('stg', 'store') }} as store
left join {{ ref('dim_staff') }} as staff on staff.staff_id = store.manager_staff_id
left join {{ ref('location_data') }} as loc on loc.address_id = store.address_id