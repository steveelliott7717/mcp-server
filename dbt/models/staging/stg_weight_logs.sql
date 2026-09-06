-- Source: health.weight_logs
with source as (select * from {{ ref('health__weight_logs') }})
select
    id::bigint          as weight_log_id,
    log_date::date      as log_date,
    weight_lbs::numeric as weight_lbs
from source
where weight_lbs is not null
