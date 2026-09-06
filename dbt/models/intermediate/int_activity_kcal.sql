-- Activity calories per day: steps + strength + cycling.
--
-- THE TWO-DAY STEP LAG IS REPRODUCED DELIBERATELY. The production RPC reads
--     WHERE date = CURRENT_DATE - INTERVAL '2 days'
-- because wearable data lands late, so the snapshot for day D is priced using
-- day D-2's step calories. That is a real modelling decision, not a bug, and
-- porting it faithfully is the point.
--
-- What this model adds is VISIBILITY: same_day_step_kcal sits alongside
-- lagged_step_kcal, so the cost of the lag is measurable for the first time.
-- In the procedure the two can never be compared, because only one is ever
-- fetched.
with spine as (
    select * from {{ ref('int_date_spine') }}
),

steps as (
    select
        activity_date,
        coalesce(sum(calories_burned), 0) as step_kcal,
        bool_or(has_actuals)              as has_actuals
    from {{ ref('stg_step_logs') }}
    group by 1
),

strength as (
    select
        scheduled_date,
        coalesce(sum(calories_burned), 0) as strength_kcal
    from {{ ref('stg_workout_logs') }}
    group by 1
),

bike as (
    select
        scheduled_date,
        coalesce(sum(calories_burned), 0) as bike_kcal
    from {{ ref('stg_bike_logs') }}
    group by 1
)

select
    s.date_day,

    -- what the production RPC actually uses (D-2)
    coalesce(lagged.step_kcal, 0)   as lagged_step_kcal,
    -- what was really walked that day, for comparison
    coalesce(sameday.step_kcal, 0)  as same_day_step_kcal,
    coalesce(sameday.has_actuals, false) as same_day_has_actuals,

    coalesce(st.strength_kcal, 0)   as strength_kcal,
    coalesce(bk.bike_kcal, 0)       as bike_kcal,

    coalesce(lagged.step_kcal, 0)
      + coalesce(st.strength_kcal, 0)
      + coalesce(bk.bike_kcal, 0)   as activity_kcal

from spine s
left join steps    as lagged  on lagged.activity_date  = s.date_day - interval 2 day
left join steps    as sameday on sameday.activity_date = s.date_day
left join strength as st      on st.scheduled_date     = s.date_day
left join bike     as bk      on bk.scheduled_date     = s.date_day
