{{
  config(
    unique_key = 'customer_id',
    )
}}
with dummy_cte as (
    select 1 from {{ ref('fact_rentals') }} limit 1
),
{% if is_incremental() %}
    target_cust as (
    select customer_id from {{ this }}
),
{% endif %}
rel_rows as (
    select 
    distinct on (customer_id)
    customer_id,
    rental_date as created_date,
    '{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
    from {{ source('target_fact', 'fact_rentals_tmp') }} as rental
    order by customer_id, rental_date
)
select 
rel_rows.*
from rel_rows
{% if is_incremental() %}
left join target_cust on rel_rows.customer_id = target_cust.customer_id
where target_cust.customer_id is null
{% endif %}