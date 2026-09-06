-- Regression test on the health_calc_strength_metrics trigger.
--
-- That trigger writes total_volume as sets × reps × weight on every insert and
-- update of health.workout_logs. stg_workout_logs recomputes it independently;
-- this asserts the two agree. If the trigger is ever changed, dropped, or
-- bypassed by a bulk load, this fails.
--
-- The production database has no equivalent check — a trigger-maintained
-- column is trusted precisely because nothing ever re-derives it.
select
    workout_log_id,
    total_volume_source,
    total_volume_calc,
    abs(total_volume_source - total_volume_calc) as drift
from {{ ref('stg_workout_logs') }}
where abs(coalesce(total_volume_source, 0) - coalesce(total_volume_calc, 0)) > 0.01
