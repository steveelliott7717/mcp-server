-- =============================================================================
-- Supabase pg_cron job definitions
-- =============================================================================
-- All jobs are scheduled via the public.f_schedule_from_cron_jobs() wrapper,
-- which upserts into cron.job. To unschedule a job by ID use:
--   SELECT public.f_unschedule_from_cron_jobs(<job_id>);
--
-- View active jobs at any time:
--   SELECT id, jobname, schedule, command, active FROM public.cron_jobs;
-- =============================================================================


-- =============================================================================
-- HEALTH
-- =============================================================================

-- Snapshot daily nutrition/fitness overview: BMR, macro targets, step calories,
-- weekly weight trend, goal adjustment. Calls rpc_snapshot_daily_overview()
-- which runs a full CTE chain (Mifflin-St Jeor BMR, 2-day lag steps, today's
-- workout calories) and upserts into health.daily_overview_snapshot.
SELECT public.f_schedule_from_cron_jobs(
  'snapshot_daily_overview',
  '30 7 * * *',
  'SELECT health.rpc_snapshot_daily_overview();'
);

-- Recalculate step calories using a 2-day lag to allow wearable data to settle.
-- Runs at 07:00 and again at 23:57 so late-arriving step data is caught same day.
SELECT public.f_schedule_from_cron_jobs(
  'calc_step_calories_two_day_lag',
  '0 7 * * *',
  'SELECT health.calc_step_calories_two_day_lag();'
);

SELECT public.f_schedule_from_cron_jobs(
  'daily-step-calories-calc',
  '57 23 * * *',
  'SELECT health.calc_step_calories_two_day_lag()'
);

-- Weekly targets: recalculate calorie/macro goals based on current body weight
-- and weekly_programs settings. Runs Monday morning before the snapshot.
SELECT public.f_schedule_from_cron_jobs(
  'adjust_weekly_targets',
  '5 7 * * 1',
  'SELECT health.rpc_adjust_weekly_targets();'
);

-- Generate the week's planned workout logs from the active weekly program.
-- Runs Monday morning after targets are adjusted.
SELECT public.f_schedule_from_cron_jobs(
  'generate_weekly_workout_logs',
  '15 7 * * 1',
  'SELECT health.generate_weekly_workout_logs();'
);


-- =============================================================================
-- FINANCE
-- =============================================================================

-- Materialise recurring purchase charges for today. Loops active
-- recurring_purchases where next_charge_date <= CURRENT_DATE, inserts a
-- charge row (idempotent), then rolls next_charge_date forward by the
-- frequency interval. See finance.generate_todays_charges().
SELECT public.f_schedule_from_cron_jobs(
  'generate_todays_charges_daily',
  '0 2 * * *',
  'SELECT finance.generate_todays_charges();'
);


-- =============================================================================
-- CALENDAR
-- =============================================================================

-- Materialise recurring calendar event instances for today and seed the next
-- occurrence. Loops active recurring_events where next_event_date <= today,
-- inserts an instance row (duplicate guard), advances next_event_date.
-- See calendar.process_recurring_events().
SELECT public.f_schedule_from_cron_jobs(
  'process_recurring_events',
  '0 7 * * *',
  'SELECT calendar.process_recurring_events();'
);


-- =============================================================================
-- MAINTENANCE / INFRA
-- =============================================================================

-- Prune idempotency cache rows older than 30 days.
SELECT public.f_schedule_from_cron_jobs(
  'idempo_cache_ttl_daily',
  '0 3 * * *',
  'DELETE FROM public.idempo_cache WHERE created_at < now() - interval ''30 days'';'
);

-- Prune agent_logs_flat rows older than 90 days via maintenance function.
SELECT public.f_schedule_from_cron_jobs(
  'prune_agent_logs_flat_daily',
  '5 3 * * *',
  'SELECT maintenance.prune_agent_logs_flat(90);'
);

-- Refresh the util schema index hourly so the MCP server always has an
-- up-to-date view of available tables, columns, and functions.
SELECT public.f_schedule_from_cron_jobs(
  'refresh-schema-index',
  '0 * * * *',
  'SELECT util.refresh_schema_index();'
);

-- Null out schema summaries every Sunday at 03:00 to force full regeneration
-- of the LLM-facing schema descriptions on the next hourly refresh.
SELECT public.f_schedule_from_cron_jobs(
  'weekly-schema-refresh',
  '0 3 * * 0',
  'UPDATE util.schema_summaries SET schema_summary = NULL;'
);
