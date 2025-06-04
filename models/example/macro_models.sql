select 
{{ macro_example('title', 'description') }} as macro_example_column,
{{ clean_string('description') }} as clean_string_macro_example
from {{ source('stg', 'film') }}