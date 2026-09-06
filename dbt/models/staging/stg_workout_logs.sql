-- Source: health.workout_logs
--
-- total_volume is recomputed here rather than trusted from source. In
-- production it is written by the health_calc_strength_metrics trigger; the
-- schema test on this model asserts the two agree, which is a regression test
-- on that trigger that did not previously exist anywhere.
with source as (select * from {{ ref('health__workout_logs') }})
select
    id::bigint               as workout_log_id,
    workout_name::varchar    as workout_name,
    exercise_id::bigint      as exercise_id,
    scheduled_date::date     as scheduled_date,
    category::varchar        as category,
    sets_completed::integer  as sets_completed,
    reps::integer            as reps,
    weight_lbs::numeric      as weight_lbs,
    rpe::numeric             as rpe,
    total_volume::numeric    as total_volume_source,
    coalesce(sets_completed, 1) * coalesce(reps, 0) * coalesce(weight_lbs, 0)
                             as total_volume_calc,
    calories_burned::numeric as calories_burned
from source
