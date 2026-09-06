{#
    Pound-to-kilogram conversion.

    The production schema uses two different constants for this — 0.4536 in
    rpc_snapshot_daily_overview() and 0.453592 in auto_calc_step_calories()
    and health_calc_strength_metrics(). That inconsistency is invisible when
    the constant is typed inline in three separate function bodies. Naming it
    once is how it became visible at all; the more precise value wins here.
#}
{% macro lbs_to_kg(lbs) %}
    ({{ lbs }} * 0.453592)
{% endmacro %}
