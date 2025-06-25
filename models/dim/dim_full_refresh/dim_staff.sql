{{
  config(
    post_hook="insert into {{this}} (staff_id, first_name,last_name) values (-1,'NA','NA')"
    )
}}

select 
staff_id,
first_name,
last_name,
email,
last_update,
case when active then 1 else 0 end as active_int,
case when active then 'yes' else 'no' end as active_desc,
'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('stg', 'staff') }}