-- Source: health.weekly_programs
--
-- RECONSTRUCTED TABLE — see README §"Reconstructed objects". Referenced by
-- rpc_snapshot_daily_overview() for `kcal_adjustment` ordered by `start_date`.
with source as (select * from {{ ref('health__weekly_programs') }})
select
    id::bigint              as weekly_program_id,
    start_date::date        as start_date,
    kcal_adjustment::integer as kcal_adjustment
from source
