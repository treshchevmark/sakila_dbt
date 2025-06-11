{% set my_column_list = ['trailers', 'deleted_scenes', 'behind_the_scenes', 'commentaries'] %}

select 
film.*,
cat.name as category_name,
lang.name as language_name,
case
    when length <= 75 then 'short'
    when length <= 120 then 'medium'
    else 'long'
end as length_desc,
'{{ run_started_at }}'::timestamp AT TIME ZONE 'UTC' as etl_time,
'{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as etl_time_str,
{% for feature in my_column_list %}
    case when film.special_features  ilike '%{{ feature }}%' then 1 else 0 end as "is_{{feature}}"
    {% if not loop.last %}, {% endif %}
{% endfor %}
from {{ source('stg', 'film') }} as film
left join {{ source('stg', 'film_category') }} as film_cat on film.film_id = film_cat.film_id
left join {{ source('stg', 'category') }} as cat on cat.category_id = film_cat.category_id
left join {{ source('stg', 'language') }} as lang on lang.language_id = film.language_id
