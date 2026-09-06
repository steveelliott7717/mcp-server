-- The macro split must sum back to the calorie target.
--
-- protein and carbs are 4 kcal/g, fat is 9 kcal/g, so:
--     protein_g*4 + fat_g*9 + carbs_g*4  ==  total_kcal_target
-- within a rounding tolerance (each macro is rounded to whole grams, so up to
-- ~9 kcal of slack is legitimate; 25 is generous).
--
-- THIS IS THE TEST THAT FOUND THE DISCREPANCY described in
-- int_calorie_targets.sql and the README. Run it against the source procedure's
-- constants and it fails by roughly 0.9 kcal per lb of bodyweight:
--
--     dbt test --select assert_macro_kcal_reconciles \
--              --vars '{fat_g_per_lb_deduction: 0.3}'
--
-- Nothing in the production schema asserts this relationship, which is why the
-- inconsistency survived in a shipped procedure.
select
    date_day,
    total_kcal_target,
    protein_g,
    fat_g,
    carbs_g,
    (protein_g * 4) + (fat_g * 9) + (carbs_g * 4) as macro_kcal,
    abs(((protein_g * 4) + (fat_g * 9) + (carbs_g * 4)) - total_kcal_target) as drift
from {{ ref('int_calorie_targets') }}
where abs(((protein_g * 4) + (fat_g * 9) + (carbs_g * 4)) - total_kcal_target) > 25
