{#
    Mifflin-St Jeor basal metabolic rate.

    Ported verbatim from health.rpc_snapshot_daily_overview()
    (supabase/schema/health.sql), which inlines this arithmetic inside a
    plpgsql CTE. Extracting it to a macro is the point of the exercise: the
    formula now has one definition, a name, and a docstring, instead of being
    a term buried in a 90-line procedure.

    Expects weight in POUNDS and height in INCHES, matching the source columns
    (health.weight_logs.weight_lbs, professional_profile.user_profile.height_in).
#}
{% macro mifflin_st_jeor(weight_lbs, height_in, age_years, sex) %}
    round(
        10   * ({{ weight_lbs }} * 0.4536)
      + 6.25 * ({{ height_in }} * 2.54)
      - 5    * {{ age_years }}
      + case when {{ sex }} = 'male' then 5 else -161 end
    , 0)
{% endmacro %}
