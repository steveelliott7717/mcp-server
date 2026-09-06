-- health.weight_logs.weight_lbs is NOT NULL but otherwise unconstrained.
-- A fat-fingered entry (18.0 for 180.0, or a kg value in a lbs column) would
-- be accepted by the database and would then propagate silently into BMR,
-- step calories and strength calories — all three read the rolling average.
select
    weight_log_id,
    log_date,
    weight_lbs
from {{ ref('stg_weight_logs') }}
where weight_lbs not between 80 and 500
