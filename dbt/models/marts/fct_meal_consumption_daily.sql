-- Daily consumption totals, aggregated from health.meal_logs.
--
-- This is the recompute half of the trade described in fct_daily_overview:
-- the same four numbers the f_update_daily_overview_consumption trigger
-- maintains incrementally on daily_overview_snapshot, derived instead.
select
    logged_date,
    count(*)                          as meals_logged,
    count(distinct recipe_id)         as distinct_recipes,
    round(sum(calories), 1)           as calories_logged,
    round(sum(protein_g), 1)          as protein_logged_g,
    round(sum(fat_g), 1)              as fat_logged_g,
    round(sum(carbs_g), 1)            as carbs_logged_g,
    round(sum(case when meal_source_type = 'recipe'    then calories else 0 end), 1) as calories_from_recipes,
    round(sum(case when meal_source_type = 'food_item' then calories else 0 end), 1) as calories_from_food_items
from {{ ref('stg_meal_logs') }}
group by 1
