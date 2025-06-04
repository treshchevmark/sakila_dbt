select 
{{ dbt_utils.star(from=ref('dim_date'), except=['weekend_indr', 'date_dim_id', 'month_name_abbreviated', 'epoch'], relation_alias = 'dim_date' ) }},
{{ dbt_utils.star(from=ref('dim_customer'), relation_alias = 'cust', suffix = '_cust', quote_identifiers=False) }}
from {{ ref('dim_date') }} as dim_date
right join {{ ref('dim_customer') }} as cust on cust.create_date = dim_date.date_key