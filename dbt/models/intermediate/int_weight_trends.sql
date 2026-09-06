-- Replaces the health.v_weight_trends VIEW.
--
-- v_weight_trends is referenced in five places across health.sql
-- (rpc_snapshot_daily_overview, health_calc_strength_metrics,
-- auto_calc_step_calories) but its definition is NOT in the repo — see README
-- §"Reconstructed objects". Reconstructed here from documented behaviour:
-- "3-day and 7-day rolling averages" over health.weight_logs.
--
-- Two deliberate improvements over a naive port:
--   1. Windows are RANGE over calendar days, not ROWS over observations, so a
--      missed weigh-in does not silently stretch a 3-day average across a week.
--   2. avg_3day_filled carries the last known value forward, so a consumer
--      asking for a gap day gets a defined answer instead of NULL. The
--      production triggers each COALESCE to a hardcoded 70.0 kg fallback
--      independently; this centralises that decision.
with spine as (
    select * from {{ ref('int_date_spine') }}
),

weights as (
    select * from {{ ref('stg_weight_logs') }}
),

joined as (
    select
        s.date_day,
        w.weight_lbs
    from spine s
    left join weights w
        on w.log_date = s.date_day
),

rolled as (
    select
        date_day,
        weight_lbs,
        avg(weight_lbs) over (
            order by date_day
            range between interval 2 day preceding and current row
        ) as avg_3day,
        avg(weight_lbs) over (
            order by date_day
            range between interval 6 day preceding and current row
        ) as avg_7day
    from joined
)

select
    date_day                                   as log_date,
    weight_lbs,
    round(avg_3day, 2)                         as avg_3day,
    round(avg_7day, 2)                         as avg_7day,
    round(
        last_value(avg_3day ignore nulls) over (
            order by date_day
            rows between unbounded preceding and current row
        ), 2
    )                                          as avg_3day_filled,
    round(
        last_value(avg_7day ignore nulls) over (
            order by date_day
            rows between unbounded preceding and current row
        ), 2
    )                                          as avg_7day_filled,
    (weight_lbs is not null)                   as has_weigh_in
from rolled
