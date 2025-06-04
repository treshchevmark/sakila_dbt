select 
*
from {{ source('stg', 'cust') }}