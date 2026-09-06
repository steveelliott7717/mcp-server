-- Source: health.recipe_ingredients
with source as (select * from {{ ref('health__recipe_ingredients') }})
select
    id::bigint           as recipe_ingredient_id,
    recipe_id::bigint    as recipe_id,
    food_item_id::bigint as food_item_id,
    quantity::numeric    as quantity,
    unit::varchar        as unit
from source
