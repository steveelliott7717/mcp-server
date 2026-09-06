-- Source: health.food_items
--
-- The production table carries ~55 nutrient columns; this project models the
-- macro subset the downstream marts actually use. Widening it is a column
-- list, not a redesign.
with source as (select * from {{ ref('health__food_items') }})
select
    id::bigint              as food_item_id,
    name::varchar           as food_name,
    nullif(brand, '')::varchar as brand,
    category::varchar       as category,
    serving_amount::numeric as serving_amount,
    serving_unit::varchar   as serving_unit,
    calories::numeric       as calories,
    protein::numeric        as protein_g,
    fat::numeric            as fat_g,
    carbs::numeric          as carbs_g
from source
