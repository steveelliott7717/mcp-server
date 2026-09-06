-- One row per day. Column-for-column the shape of
-- health.daily_overview_snapshot, so this mart is directly comparable to the
-- table the production cron writes.
--
-- Two structural differences from the procedure that produces that table:
--
--   1. GRAIN. rpc_snapshot_daily_overview() upserts a single row for
--      CURRENT_DATE. This computes every day on the spine, so a formula fix
--      reaches history rather than only tomorrow.
--
--   2. CONSUMPTION. In production, calories_logged and friends are maintained
--      incrementally by the f_update_daily_overview_consumption trigger, which
--      fires on every meal_logs insert and adds to a running total. That is
--      fast but has no way to self-heal: a deleted or edited meal row leaves
--      the total permanently wrong, because nothing ever recomputes it. Here
--      the totals are derived by aggregation, so they are correct by
--      construction on every run. That is the classic incremental-vs-recompute
--      trade, made explicit.
with targets as (
    select * from {{ ref('int_calorie_targets') }}
),

consumption as (
    select * from {{ ref('fct_meal_consumption_daily') }}
),

trend as (
    select
        date_day,
        avg_7day,
        lag(avg_7day, 7) over (order by date_day) as avg_7day_prior
    from targets
)

select
    t.date_day                                    as snapshot_date,

    -- targets
    t.bmr_kcal,
    t.activity_kcal,
    t.goal_adjustment_kcal,
    t.total_kcal_target,

    -- macro targets
    t.protein_g,
    t.fat_g,
    t.carbs_g,

    -- weight trends
    t.avg_3day,
    t.avg_7day,
    round(coalesce(tr.avg_7day - tr.avg_7day_prior, 0), 2) as weekly_gain_lbs,

    -- consumption (recomputed, not accumulated)
    coalesce(c.calories_logged, 0)  as calories_logged,
    coalesce(c.protein_logged_g, 0) as protein_logged_g,
    coalesce(c.fat_logged_g, 0)     as fat_logged_g,
    coalesce(c.carbs_logged_g, 0)   as carbs_logged_g,

    -- derived: the question the snapshot table cannot answer without a join
    round(coalesce(c.calories_logged, 0) - t.total_kcal_target, 1) as kcal_balance,
    coalesce(c.meals_logged, 0)     as meals_logged

from targets t
left join consumption c on c.logged_date = t.date_day
left join trend tr      on tr.date_day   = t.date_day
