{{
  config(
    materialized = 'incremental',
    unique_key = 'rental_id',
    indexes= [{'columns': ['rental_id']}]
    )
}}


{% if is_incremental() %}
 

{% set query %}
select (max(rental_id) - {{ var('retro_rental_id') }}) as last_rental_id from {{ this }} 
{% endset %}
{% set last_rental_id = run_query(query).columns[0][0] %}

{% endif %}

select 
*
from stg.rental_big_table
{% if is_incremental() %}
  where rental_id >= {{ last_rental_id }}
{% endif %}