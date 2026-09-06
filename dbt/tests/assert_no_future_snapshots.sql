-- The spine is derived from observed log dates, so a row dated in the future
-- means a log table contains a future-dated entry — worth surfacing rather
-- than quietly averaging into a trend.
select
    snapshot_date
from {{ ref('fct_daily_overview') }}
where snapshot_date > current_date
