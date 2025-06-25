{% macro inc_rental() %}


drop table if exists {{ source('target_fact', 'fact_rentals_tmp') }};
create table if not exists {{ source('target_fact', 'fact_rentals_tmp') }} as
with refresh_date as (
    select 
    from_date
    from {{ source('stg', 'z_refresh_from') }}
    where table_name = '{{this}}' and to_refresh = 1
)
select 
*,
'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str
from {{ source('stg', 'rental') }} as rental
{% if is_incremental() %}
  where rental.rental_date >= coalesce(
                                      (select from_date from refresh_date), 
                                      (select max(rental_date) from {{ source('target_fact', 'fact_rentals') }}), 
                                      '{{ var('init_date') }}'
);
{% endif %}
{% endmacro %}