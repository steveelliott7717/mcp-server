-- Source: health.bike_logs
--
-- RECONSTRUCTED TABLE. health.bike_logs is referenced by
-- rpc_snapshot_daily_overview() but has no CREATE TABLE in
-- supabase/schema/health.sql. Columns below are inferred from that usage
-- (scheduled_date, calories_burned) plus one obvious companion field.
-- See README §"Reconstructed objects".
with source as (select * from {{ ref('health__bike_logs') }})
select
    id::bigint               as bike_log_id,
    scheduled_date::date     as scheduled_date,
    duration_minutes::integer as duration_minutes,
    calories_burned::numeric as calories_burned
from source
