{{
  config(
    materialized = 'incremental',
    unique_key = 'rental_id',
    pre_hook = '{{ inc_rental() }}'
    )
}}

select 
rental.rental_id,
rental.rental_date,
case when film.film_id is not null then film.film_id else -1 end as film_id,
case when cust.customer_id is not null then cust.customer_id else -1 end as customer_id,
case when staff.staff_id is not null then staff.staff_id else -1 end as staff_id,
case when store.store_id is not null then store.store_id else -1 end as store_id,
to_char(rental_date, 'YYYYMMDD')::int as date_key,
case when return_date is null then 0 else 1 end as is_return,
extract(epoch from (rental_date - return_date))/60 as return_date_hr,
'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('target_fact', 'fact_rentals_tmp') }} as rental
left join {{ source('stg', 'inventory') }} as inv on inv.inventory_id = rental.inventory_id
left join {{ ref('dim_customer') }}  as cust on rental.customer_id = cust.customer_id
left join {{ ref('dim_staff') }} as staff on staff.staff_id = rental.staff_id
left join {{ ref('dim_store') }} as store on store.store_id = inv.store_id
left join {{ ref('dim_film') }} as film on film.film_id = inv.film_id