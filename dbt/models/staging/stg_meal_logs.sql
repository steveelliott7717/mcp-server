-- Source: health.meal_logs
--
-- Macros arrive pre-scaled by quantity (f_fill_meal_macros trigger), so they
-- are summed as-is rather than multiplied again.
with source as (select * from {{ ref('health__meal_logs') }})
select
    id::bigint          as meal_log_id,
    recipe_id::bigint   as recipe_id,
    food_item_id::bigint as food_item_id,
    logged_date::date   as logged_date,
    quantity::numeric   as quantity,
    calories::numeric   as calories,
    protein_g::numeric  as protein_g,
    fat_g::numeric      as fat_g,
    carbs_g::numeric    as carbs_g,
    case
        when recipe_id is not null then 'recipe'
        when food_item_id is not null then 'food_item'
        else 'unknown'
    end as meal_source_type
from source
