-- Source: health.step_logs
--
-- `steps` is the estimate/target; `actual_steps_taken` is backfilled by a job
-- on a two-day lag and is NULL until then. Keeping both, plus an explicit
-- has_actuals flag, so downstream models never silently treat a pending day
-- as a zero-step day.
with source as (select * from {{ ref('health__step_logs') }})
select
    id::bigint                  as step_log_id,
    "date"::date                as activity_date,
    steps::integer              as steps_target,
    steps_agg::integer          as steps_aggregated,
    actual_steps_taken::integer as actual_steps_taken,
    calories_burned::numeric    as calories_burned,
    (actual_steps_taken is not null) as has_actuals
from source
