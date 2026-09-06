-- A contiguous daily spine covering every date any log table touches.
--
-- The production RPC has no spine: it computes CURRENT_DATE only, one row per
-- cron run, and cannot be backfilled. Building on a spine is the single
-- biggest behavioural difference between this project and the procedure it
-- ports — every historical day is recomputed on every run, so a corrected
-- formula reaches history instead of only tomorrow.
with bounds as (
    select
        least(
            (select min(log_date)       from {{ ref('stg_weight_logs') }}),
            (select min(activity_date)  from {{ ref('stg_step_logs') }}),
            (select min(scheduled_date) from {{ ref('stg_workout_logs') }})
        ) as min_date,
        greatest(
            (select max(log_date)       from {{ ref('stg_weight_logs') }}),
            (select max(activity_date)  from {{ ref('stg_step_logs') }}),
            (select max(scheduled_date) from {{ ref('stg_workout_logs') }})
        ) as max_date
    from (select 1) as _
)
select cast(g.generate_series as date) as date_day
from bounds
cross join generate_series(bounds.min_date, bounds.max_date, interval 1 day) as g
