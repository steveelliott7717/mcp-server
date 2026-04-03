-- =============================================================================
-- Schema: calendar
-- Description: Calendar system with one-off events and recurring events.
--              Recurring events use a template + materialized instances pattern:
--              recurring_events holds the schedule definition, recurring_event_instances
--              holds the concrete occurrences. A cron job calls
--              process_recurring_events() to roll the schedule forward.
--              All events carry Pushover notification preferences tracked per-instance.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS calendar;

-- =============================================================================
-- TABLE: calendar.events
-- One-off calendar events. Also the target for cross-schema inserts from the
-- genealogy schema (auto_create_document_calendar_events trigger).
-- =============================================================================

CREATE TABLE calendar.events (
  id          bigint      NOT NULL DEFAULT nextval('calendar.events_id_seq'),
  title       text        NOT NULL,
  description text,
  start_time  timestamptz NOT NULL,
  end_time    timestamptz,
  location    text,

  -- Pushover notification preferences
  notify_before_event         boolean   DEFAULT true,
  notify_before_event_minutes integer   DEFAULT 30,
  pushover_sent_before_event  timestamptz,        -- stamped when notification sent

  notify_on_the_day           boolean   DEFAULT true,
  notify_on_the_day_time      time      DEFAULT '16:00:00',
  pushover_sent_on_the_day    timestamptz,

  notify_at_start             boolean   DEFAULT false,
  pushover_sent_at_start      timestamptz,

  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now(),

  CONSTRAINT events_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: calendar.recurring_events
-- Template/schedule definition for recurring events. Supports daily, weekly,
-- biweekly, monthly, quarterly, and yearly frequencies.
--
-- next_event_date tracks the leading edge of materialized instances and is
-- advanced forward by process_recurring_events() on each cron run.
-- linked_event_id optionally ties this schedule to a one-off events row.
-- =============================================================================

CREATE TABLE calendar.recurring_events (
  id          bigint      NOT NULL DEFAULT nextval('calendar.recurring_events_id_seq'),
  title       text        NOT NULL,
  description text,
  location    text,
  start_time  timestamptz NOT NULL,
  end_time    timestamptz,

  -- Notification preferences (copied to each instance on creation)
  notify_before_event         boolean   DEFAULT true,
  notify_before_event_minutes integer   DEFAULT 30,
  notify_on_the_day           boolean   DEFAULT true,
  notify_on_the_day_time      time      DEFAULT '16:00:00',
  notify_at_start             boolean   DEFAULT false,

  -- Schedule
  frequency           text    NOT NULL DEFAULT 'monthly',  -- daily|weekly|biweekly|monthly|quarterly|yearly
  start_event_date    date    NOT NULL DEFAULT CURRENT_DATE,
  next_event_date     date,       -- leading edge; advanced by process_recurring_events()
  active              boolean NOT NULL DEFAULT true,
  cancelled_date      timestamptz,    -- stamped when deactivated

  linked_event_id     bigint,     -- optional link to a one-off events row

  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT recurring_events_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: calendar.recurring_event_instances
-- Materialized occurrences of a recurring_events schedule. Created two at a
-- time (current + next) so the calendar always has a lookahead. Notification
-- sent timestamps are tracked per-instance so each occurrence is independent.
-- =============================================================================

CREATE TABLE calendar.recurring_event_instances (
  id                  bigint      NOT NULL DEFAULT nextval('calendar.recurring_event_instances_id_seq'),
  recurring_event_id  bigint      NOT NULL,
  title               text        NOT NULL,
  description         text,
  location            text,
  start_time          timestamptz NOT NULL,
  end_time            timestamptz,

  -- Notification preferences (snapshot from template at creation time)
  notify_before_event         boolean   DEFAULT true,
  notify_before_event_minutes integer   DEFAULT 30,
  notify_on_the_day           boolean   DEFAULT true,
  notify_on_the_day_time      time      DEFAULT '16:00:00',
  notify_at_start             boolean   DEFAULT false,

  -- Pushover delivery tracking (per-instance)
  pushover_sent_before_event  timestamptz,
  pushover_sent_on_the_day    timestamptz,
  pushover_sent_at_start      timestamptz,

  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT recurring_event_instances_pkey PRIMARY KEY (id),
  CONSTRAINT recurring_event_instances_recurring_event_id_fkey
    FOREIGN KEY (recurring_event_id)
    REFERENCES calendar.recurring_events(id) ON DELETE CASCADE
);

-- =============================================================================
-- TABLE: calendar.notifications
-- Generic notification log (category-tagged, used for ad-hoc alerts).
-- =============================================================================

CREATE TABLE calendar.notifications (
  id          bigint      NOT NULL,
  title       text,
  description text,
  category    text,
  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now(),

  CONSTRAINT notifications_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION calendar.set_created_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.created_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION calendar.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- calendar.seed_first_recurring_instance()
--
-- AFTER INSERT on recurring_events. When a new schedule is created with
-- active=true and next_event_date set, immediately materializes two instances
-- (current occurrence + one period ahead) so the calendar always has a
-- lookahead. Advances next_event_date forward by one period.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION calendar.seed_first_recurring_instance()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  period INTERVAL;
BEGIN
  IF NEW.active = TRUE AND NEW.next_event_date IS NOT NULL THEN

    CASE lower(NEW.frequency)
      WHEN 'daily'     THEN period := INTERVAL '1 day';
      WHEN 'weekly'    THEN period := INTERVAL '1 week';
      WHEN 'biweekly'  THEN period := INTERVAL '2 weeks';
      WHEN 'monthly'   THEN period := INTERVAL '1 month';
      WHEN 'quarterly' THEN period := INTERVAL '3 months';
      WHEN 'yearly'    THEN period := INTERVAL '1 year';
      ELSE RAISE EXCEPTION 'Invalid frequency: %', NEW.frequency;
    END CASE;

    -- Seed current occurrence
    INSERT INTO calendar.recurring_event_instances (
      recurring_event_id, title, description, location,
      start_time, end_time,
      notify_before_event, notify_before_event_minutes,
      notify_on_the_day, notify_on_the_day_time, notify_at_start
    ) VALUES (
      NEW.id, NEW.title, NEW.description, NEW.location,
      (NEW.next_event_date + NEW.start_time::time),
      CASE WHEN NEW.end_time IS NOT NULL THEN (NEW.next_event_date + NEW.end_time::time) ELSE NULL END,
      NEW.notify_before_event, NEW.notify_before_event_minutes,
      NEW.notify_on_the_day, NEW.notify_on_the_day_time, NEW.notify_at_start
    );

    -- Seed next occurrence (lookahead)
    INSERT INTO calendar.recurring_event_instances (
      recurring_event_id, title, description, location,
      start_time, end_time,
      notify_before_event, notify_before_event_minutes,
      notify_on_the_day, notify_on_the_day_time, notify_at_start
    ) VALUES (
      NEW.id, NEW.title, NEW.description, NEW.location,
      (NEW.next_event_date + period + NEW.start_time::time),
      CASE WHEN NEW.end_time IS NOT NULL THEN (NEW.next_event_date + period + NEW.end_time::time) ELSE NULL END,
      NEW.notify_before_event, NEW.notify_before_event_minutes,
      NEW.notify_on_the_day, NEW.notify_on_the_day_time, NEW.notify_at_start
    );

    -- Advance the leading edge
    UPDATE calendar.recurring_events
    SET next_event_date = NEW.next_event_date + period
    WHERE id = NEW.id;

  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- calendar.process_recurring_events()
--
-- Called by a cron job (not a trigger). Scans all active recurring events
-- where next_event_date <= today, generates the next instance with a
-- duplicate guard (WHERE NOT EXISTS), and advances next_event_date forward.
-- This is the heartbeat that keeps the rolling schedule populated.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION calendar.process_recurring_events()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r                    RECORD;
  period               INTERVAL;
  next_occurrence_date DATE;
BEGIN
  FOR r IN
    SELECT * FROM calendar.recurring_events
    WHERE active = TRUE
      AND next_event_date <= CURRENT_DATE
  LOOP
    CASE lower(r.frequency)
      WHEN 'daily'     THEN period := INTERVAL '1 day';
      WHEN 'weekly'    THEN period := INTERVAL '1 week';
      WHEN 'biweekly'  THEN period := INTERVAL '2 weeks';
      WHEN 'monthly'   THEN period := INTERVAL '1 month';
      WHEN 'quarterly' THEN period := INTERVAL '3 months';
      WHEN 'yearly'    THEN period := INTERVAL '1 year';
      ELSE RAISE EXCEPTION 'Invalid frequency: %', r.frequency;
    END CASE;

    next_occurrence_date := r.next_event_date + period;

    -- Insert next instance only if it doesn't already exist
    IF NOT EXISTS (
      SELECT 1 FROM calendar.recurring_event_instances
      WHERE recurring_event_id = r.id
        AND start_time::date = next_occurrence_date
    ) THEN
      INSERT INTO calendar.recurring_event_instances (
        recurring_event_id, title, description, location,
        start_time, end_time,
        notify_before_event, notify_before_event_minutes,
        notify_on_the_day, notify_on_the_day_time, notify_at_start
      ) VALUES (
        r.id, r.title, r.description, r.location,
        (next_occurrence_date + r.start_time::time),
        CASE WHEN r.end_time IS NOT NULL THEN (next_occurrence_date + r.end_time::time) ELSE NULL END,
        r.notify_before_event, r.notify_before_event_minutes,
        r.notify_on_the_day, r.notify_on_the_day_time, r.notify_at_start
      );
    END IF;

    UPDATE calendar.recurring_events
    SET next_event_date = next_occurrence_date,
        updated_at = now()
    WHERE id = r.id;

  END LOOP;
END;
$$;

-- -----------------------------------------------------------------------------
-- calendar.handle_recurring_event_active_change()
--
-- BEFORE UPDATE on recurring_events. Handles two transitions:
--
--   Deactivate (true → false): stamps cancelled_date, clears next_event_date.
--
--   Reactivate (false → true): walks forward from start_event_date in period
--   multiples to find the next future occurrence, seeds it (and a second
--   lookahead instance if the cron won't catch it in time), then sets
--   next_event_date to the correct resumption point.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION calendar.handle_recurring_event_active_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  period    INTERVAL;
  next_date DATE;
BEGIN
  -- Deactivate
  IF OLD.active = TRUE AND NEW.active = FALSE THEN
    NEW.cancelled_date  := now();
    NEW.next_event_date := NULL;
  END IF;

  -- Reactivate
  IF OLD.active = FALSE AND NEW.active = TRUE THEN
    NEW.cancelled_date := NULL;

    CASE lower(NEW.frequency)
      WHEN 'daily'     THEN period := INTERVAL '1 day';
      WHEN 'weekly'    THEN period := INTERVAL '1 week';
      WHEN 'biweekly'  THEN period := INTERVAL '2 weeks';
      WHEN 'monthly'   THEN period := INTERVAL '1 month';
      WHEN 'quarterly' THEN period := INTERVAL '3 months';
      WHEN 'yearly'    THEN period := INTERVAL '1 year';
      ELSE period := INTERVAL '1 month';
    END CASE;

    -- Walk forward from start_event_date until we're past today
    next_date := NEW.start_event_date;
    WHILE next_date <= CURRENT_DATE LOOP
      next_date := next_date + period;
    END LOOP;

    -- Seed the next occurrence (duplicate-safe)
    INSERT INTO calendar.recurring_event_instances (
      recurring_event_id, title, description, location,
      start_time, end_time,
      notify_before_event, notify_before_event_minutes,
      notify_on_the_day, notify_on_the_day_time, notify_at_start
    )
    SELECT
      NEW.id, NEW.title, NEW.description, NEW.location,
      (next_date + NEW.start_time::time),
      CASE WHEN NEW.end_time IS NOT NULL THEN (next_date + NEW.end_time::time) ELSE NULL END,
      NEW.notify_before_event, NEW.notify_before_event_minutes,
      NEW.notify_on_the_day, NEW.notify_on_the_day_time, NEW.notify_at_start
    WHERE NOT EXISTS (
      SELECT 1 FROM calendar.recurring_event_instances
      WHERE recurring_event_id = NEW.id AND start_time::date = next_date
    );

    -- If next_date is today or past, cron won't seed the following instance — do it now
    IF next_date <= CURRENT_DATE THEN
      INSERT INTO calendar.recurring_event_instances (
        recurring_event_id, title, description, location,
        start_time, end_time,
        notify_before_event, notify_before_event_minutes,
        notify_on_the_day, notify_on_the_day_time, notify_at_start
      )
      SELECT
        NEW.id, NEW.title, NEW.description, NEW.location,
        (next_date + period + NEW.start_time::time),
        CASE WHEN NEW.end_time IS NOT NULL THEN (next_date + period + NEW.end_time::time) ELSE NULL END,
        NEW.notify_before_event, NEW.notify_before_event_minutes,
        NEW.notify_on_the_day, NEW.notify_on_the_day_time, NEW.notify_at_start
      WHERE NOT EXISTS (
        SELECT 1 FROM calendar.recurring_event_instances
        WHERE recurring_event_id = NEW.id AND start_time::date = next_date + period
      );

      NEW.next_event_date := next_date + period;
    ELSE
      NEW.next_event_date := next_date;
    END IF;

  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- calendar.attach_timestamp_triggers()
-- DDL event trigger — auto-attaches timestamp triggers to new tables.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION calendar.attach_timestamp_triggers()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
  obj            RECORD;
  has_created_at boolean;
  has_updated_at boolean;
  t_name         text;
  s_name         text;
  trigger_exists boolean;
BEGIN
  FOR obj IN
    SELECT * FROM pg_event_trigger_ddl_commands()
    WHERE command_tag = 'CREATE TABLE'
  LOOP
    s_name := (SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = obj.objid);
    t_name := (SELECT c.relname FROM pg_class c WHERE c.oid = obj.objid);

    IF s_name != 'calendar' THEN CONTINUE; END IF;

    SELECT EXISTS (SELECT 1 FROM information_schema.columns
      WHERE table_schema = s_name AND table_name = t_name AND column_name = 'created_at')
      INTO has_created_at;

    SELECT EXISTS (SELECT 1 FROM information_schema.columns
      WHERE table_schema = s_name AND table_name = t_name AND column_name = 'updated_at')
      INTO has_updated_at;

    IF has_created_at THEN
      SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_force_created_at'
        AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass)
        INTO trigger_exists;
      IF NOT trigger_exists THEN
        EXECUTE format('CREATE TRIGGER trg_force_created_at BEFORE INSERT ON %I.%I
          FOR EACH ROW EXECUTE FUNCTION calendar.set_created_at();', s_name, t_name);
      END IF;
    END IF;

    IF has_updated_at THEN
      SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = format('trg_%s_updated_at', t_name)
        AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass)
        INTO trigger_exists;
      IF NOT trigger_exists THEN
        EXECUTE format('CREATE TRIGGER trg_%I_updated_at BEFORE UPDATE ON %I.%I
          FOR EACH ROW EXECUTE FUNCTION calendar.set_updated_at();', t_name, s_name, t_name);
      END IF;
    END IF;
  END LOOP;
END;
$$;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- events
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON calendar.events
  FOR EACH ROW EXECUTE FUNCTION calendar.set_created_at();

CREATE TRIGGER trg_events_updated_at
  BEFORE UPDATE ON calendar.events
  FOR EACH ROW EXECUTE FUNCTION calendar.set_updated_at();

-- recurring_events
CREATE TRIGGER trg_seed_first_recurring_instance
  AFTER INSERT ON calendar.recurring_events
  FOR EACH ROW EXECUTE FUNCTION calendar.seed_first_recurring_instance();

CREATE TRIGGER trg_recurring_events_active_change
  BEFORE UPDATE ON calendar.recurring_events
  FOR EACH ROW EXECUTE FUNCTION calendar.handle_recurring_event_active_change();

CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON calendar.recurring_events
  FOR EACH ROW EXECUTE FUNCTION calendar.set_created_at();

CREATE TRIGGER trg_recurring_events_updated_at
  BEFORE UPDATE ON calendar.recurring_events
  FOR EACH ROW EXECUTE FUNCTION calendar.set_updated_at();

-- recurring_event_instances
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON calendar.recurring_event_instances
  FOR EACH ROW EXECUTE FUNCTION calendar.set_created_at();

CREATE TRIGGER trg_recurring_event_instances_updated_at
  BEFORE UPDATE ON calendar.recurring_event_instances
  FOR EACH ROW EXECUTE FUNCTION calendar.set_updated_at();

-- notifications
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON calendar.notifications
  FOR EACH ROW EXECUTE FUNCTION calendar.set_created_at();

CREATE TRIGGER trg_notifications_updated_at
  BEFORE UPDATE ON calendar.notifications
  FOR EACH ROW EXECUTE FUNCTION calendar.set_updated_at();

-- =============================================================================
-- EVENT TRIGGER
-- Auto-attaches timestamp triggers to any new table in the calendar schema
-- =============================================================================

CREATE EVENT TRIGGER calendar_timestamp_on_create
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION calendar.attach_timestamp_triggers();

-- =============================================================================
-- CRON
-- process_recurring_events() is called on a schedule to roll the recurring
-- event instances forward. Example pg_cron registration:
--
--   SELECT cron.schedule('process-recurring-events', '0 6 * * *',
--     'SELECT calendar.process_recurring_events()');
-- =============================================================================
