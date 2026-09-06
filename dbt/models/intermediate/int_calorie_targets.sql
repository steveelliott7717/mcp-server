-- Calorie and macro targets per day.
--
-- This is the arithmetic core of health.rpc_snapshot_daily_overview(), lifted
-- out of the procedure and given a per-day grain.
--
-- ---------------------------------------------------------------------------
-- DISCREPANCY FOUND DURING THE PORT — see README §"What the port surfaced".
--
-- The source procedure states its macro policy in comments as:
--     protein = 1g per lb bodyweight
--     fat     = 0.4g per lb bodyweight
--     carbs   = remaining calories / 4
-- and computes protein_g and fat_g exactly that way. But the carbohydrate
-- backsolve subtracts fat calories using 0.3, not 0.4:
--     ROUND((... - (weight_lbs * 4 + weight_lbs * 0.3 * 9)) / 4, 0)
--
-- So the fat grams it PRESCRIBES (0.4/lb) and the fat calories it DEDUCTS
-- (0.3/lb) disagree, and carbohydrates absorb the difference — roughly
-- 0.9 kcal per lb of bodyweight, about 160 kcal/day at 180 lb, silently
-- inflating the carb target and pushing the macro split past the calorie
-- target it is supposed to sum to.
--
-- This model uses the CONSISTENT value (0.4 in both places), which is what
-- makes tests/assert_macro_kcal_reconciles.sql pass. Setting
-- `vars: {fat_g_per_lb_deduction: 0.3}` reproduces the source behaviour
-- exactly, and that test then fails — which is the demonstration.
-- ---------------------------------------------------------------------------
{% set protein_g_per_lb   = var('protein_g_per_lb', 1.0) %}
{% set fat_g_per_lb       = var('fat_g_per_lb', 0.4) %}
{% set fat_g_per_lb_dedux = var('fat_g_per_lb_deduction', fat_g_per_lb) %}

with spine as (
    select * from {{ ref('int_date_spine') }}
),

profile as (
    select * from {{ ref('stg_user_profile') }} limit 1
),

trends as (
    select * from {{ ref('int_weight_trends') }}
),

activity as (
    select * from {{ ref('int_activity_kcal') }}
),

-- Most recent program in effect on each day. The procedure takes the newest
-- row overall (ORDER BY start_date DESC LIMIT 1), which is only correct for
-- today; on a spine it has to be as-of the day being computed.
goal as (
    select
        s.date_day,
        (
            select wp.kcal_adjustment
            from {{ ref('stg_weekly_programs') }} wp
            where wp.start_date <= s.date_day
            order by wp.start_date desc
            limit 1
        ) as goal_adjustment_kcal
    from spine s
),

base as (
    select
        s.date_day,
        t.avg_3day_filled                     as weight_lbs,
        t.avg_7day_filled                     as avg_7day,
        t.avg_3day                            as avg_3day,
        p.height_in,
        date_diff('year', p.date_of_birth, s.date_day) as age_years,
        p.sex,
        a.activity_kcal,
        coalesce(g.goal_adjustment_kcal, 0)   as goal_adjustment_kcal
    from spine s
    cross join profile p
    left join trends   t on t.log_date  = s.date_day
    left join activity a on a.date_day  = s.date_day
    left join goal     g on g.date_day  = s.date_day
),

calc as (
    select
        date_day,
        weight_lbs,
        avg_3day,
        avg_7day,
        activity_kcal,
        goal_adjustment_kcal,

        {{ mifflin_st_jeor('weight_lbs', 'height_in', 'age_years', 'sex') }} as bmr_kcal,

        round(weight_lbs * {{ protein_g_per_lb }}, 0) as protein_g,
        round(weight_lbs * {{ fat_g_per_lb }}, 0)     as fat_g
    from base
)

select
    date_day,
    weight_lbs,
    avg_3day,
    avg_7day,
    bmr_kcal,
    activity_kcal,
    goal_adjustment_kcal,
    bmr_kcal + activity_kcal + goal_adjustment_kcal as total_kcal_target,
    protein_g,
    fat_g,
    round(
        (
            (bmr_kcal + activity_kcal + goal_adjustment_kcal)
            - (weight_lbs * {{ protein_g_per_lb }} * 4)
            - (weight_lbs * {{ fat_g_per_lb_dedux }} * 9)
        ) / 4, 0
    ) as carbs_g
from calc
where weight_lbs is not null
