{{
  config(
    materialized = 'incremental',
    unique_key = 'payment_id',
    )
}}

select 
*
from {{ source('stg', 'payment') }}
{% if is_incremental() %}
  and payment_date >= coalesce((select max(payment_date) from {{ this }}), '1900-01-01')
{% endif %}