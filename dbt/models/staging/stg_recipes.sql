-- Source: health.recipes
with source as (select * from {{ ref('health__recipes') }})
select
    id::bigint             as recipe_id,
    name::varchar          as recipe_name,
    category::varchar      as category,
    total_calories::numeric as total_calories,
    protein_g::numeric     as protein_g,
    fat_g::numeric         as fat_g,
    carbs_g::numeric       as carbs_g
from source
