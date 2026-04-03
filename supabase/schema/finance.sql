-- =============================================================================
-- Schema: finance
-- Description: Purchase tracking (one-off and recurring) with cross-schema
--              links to health.food_items and health.household_items, and a
--              cron-driven charge materialisation pipeline for subscriptions.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS finance;

-- =============================================================================
-- TABLE: finance.purchases
-- Description: One-off purchases. Links optionally to health.food_items or
--              health.household_items via a mutual-exclusion CHECK constraint
--              (at most one item FK active at a time). On INSERT the
--              trg_add_inventory_on_purchase trigger calls
--              health.add_inventory_on_purchase() to update stock levels.
-- =============================================================================

CREATE TABLE finance.purchases (
  id          bigint  NOT NULL GENERATED ALWAYS AS IDENTITY,
  category    text    NOT NULL DEFAULT 'general',
  vendor      text,
  item_name   text    NOT NULL,

  -- Cross-schema item references (mutually exclusive — see chk_one_item_id)
  food_item_id      bigint,   -- → health.food_items(id)   ON DELETE SET NULL
  household_item_id bigint,   -- → health.household_items(id) ON DELETE SET NULL

  quantity    numeric DEFAULT 1,

  -- Cost breakdown
  base_cost       numeric,
  shipping_cost   numeric DEFAULT 0,
  tax_cost        numeric DEFAULT 0,
  other_cost      numeric DEFAULT 0,

  purchase_date   date    NOT NULL DEFAULT CURRENT_DATE,
  notes           text,

  -- Fulfilment
  order_number            text,
  tracking_number         text,
  carrier                 text,
  expected_delivery_start date,
  expected_delivery_end   date,
  receipt_image_url       text,

  -- Payment
  card_type   text    DEFAULT 'Visa',
  card_last4  char(4),

  -- Meta
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT purchases_pkey PRIMARY KEY (id),

  -- Exactly one item FK may be set, or neither — never both
  CONSTRAINT chk_one_item_id CHECK (
    (food_item_id IS NOT NULL AND household_item_id IS NULL)
    OR (household_item_id IS NOT NULL AND food_item_id IS NULL)
    OR (food_item_id IS NULL AND household_item_id IS NULL)
  ),

  CONSTRAINT purchases_card_last4_check
    CHECK (card_last4 ~ '^[0-9]{4}$' OR card_last4 IS NULL),

  CONSTRAINT purchases_food_item_id_fkey
    FOREIGN KEY (food_item_id)
    REFERENCES health.food_items (id) ON DELETE SET NULL,

  CONSTRAINT purchases_household_item_id_fkey
    FOREIGN KEY (household_item_id)
    REFERENCES health.household_items (id) ON DELETE SET NULL
);

CREATE INDEX idx_purchases_category ON finance.purchases USING btree (category);
CREATE INDEX idx_purchases_date     ON finance.purchases USING btree (purchase_date);
CREATE INDEX idx_purchases_item_id  ON finance.purchases USING btree (food_item_id);

-- =============================================================================
-- TABLE: finance.recurring_purchases
-- Description: Subscription / recurring billing template. Tracks the schedule
--              (frequency, start/next charge + invoice dates) and active state.
--              generate_todays_charges() is called by cron to materialise
--              recurring_purchase_charges rows from this template.
-- =============================================================================

CREATE TABLE finance.recurring_purchases (
  id          bigint  NOT NULL GENERATED ALWAYS AS IDENTITY,
  category    text    NOT NULL DEFAULT 'general',
  vendor      text,
  item_name   text    NOT NULL,
  item_id     bigint,
  quantity    numeric DEFAULT 1,

  -- Cost breakdown (copied to each charge on materialisation)
  base_cost     numeric,
  shipping_cost numeric DEFAULT 0,
  tax_cost      numeric DEFAULT 0,
  other_cost    numeric DEFAULT 0,

  notes         text,

  -- Schedule
  frequency           text    NOT NULL DEFAULT 'monthly',
                              -- daily | weekly | biweekly | monthly | quarterly | yearly
  start_charge_date   date    NOT NULL DEFAULT CURRENT_DATE,
  next_charge_date    date,
  start_invoice_date  date,
  next_invoice_date   date,

  -- State
  active          boolean     NOT NULL DEFAULT true,
  cancelled_date  timestamptz,          -- stamped by trigger on deactivation

  -- Optional back-link to the original one-time purchase that started this
  -- subscription (e.g. the signup transaction)
  linked_purchase_id  bigint,           -- → finance.purchases(id)

  -- Payment
  card_type   text    DEFAULT 'Mastercard',
  card_last4  char(4),

  -- Meta
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT recurring_purchases_pkey PRIMARY KEY (id),

  CONSTRAINT recurring_purchases_frequency_check
    CHECK (frequency = ANY (ARRAY[
      'daily', 'weekly', 'biweekly', 'monthly', 'quarterly', 'yearly'
    ])),

  CONSTRAINT recurring_purchases_card_last4_check
    CHECK (card_last4 ~ '^[0-9]{4}$' OR card_last4 IS NULL),

  CONSTRAINT recurring_purchases_linked_purchase_id_fkey
    FOREIGN KEY (linked_purchase_id)
    REFERENCES finance.purchases (id)
);

-- =============================================================================
-- TABLE: finance.recurring_purchase_charges
-- Description: Materialised charge instances produced by
--              generate_todays_charges(). One row per billing cycle per
--              subscription. manually_overridden = true means the amount or
--              dates were hand-edited and should not be recalculated.
-- =============================================================================

CREATE TABLE finance.recurring_purchase_charges (
  id                    bigint  NOT NULL GENERATED ALWAYS AS IDENTITY,
  recurring_purchase_id bigint  NOT NULL,
  vendor                text,
  item_name             text,
  charge_date           date    NOT NULL,
  invoice_date          date,

  -- Cost (copied from template at materialisation time, editable after)
  base_cost     numeric,
  tax_cost      numeric DEFAULT 0,
  other_cost    numeric DEFAULT 0,

  notes                 text,
  manually_overridden   boolean DEFAULT false,
  receipt_image_url     text,

  -- Meta
  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now(),

  CONSTRAINT recurring_purchase_charges_pkey PRIMARY KEY (id),

  CONSTRAINT recurring_purchase_charges_recurring_purchase_id_fkey
    FOREIGN KEY (recurring_purchase_id)
    REFERENCES finance.recurring_purchases (id) ON DELETE CASCADE
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Timestamp helpers
CREATE OR REPLACE FUNCTION finance.set_created_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.created_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION finance.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- finance.normalize_frequency_lower()
--
-- BEFORE INSERT OR UPDATE on recurring_purchases. Lowercases frequency so the
-- CHECK constraint and CASE statements in the scheduling functions receive a
-- consistent value regardless of how the caller supplied it.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION finance.normalize_frequency_lower()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.frequency IS NOT NULL THEN
    NEW.frequency := lower(NEW.frequency);
  END IF;
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- finance.handle_recurring_purchase_active_change()
--
-- BEFORE UPDATE on recurring_purchases. Manages the active ↔ inactive state
-- transition:
--
--   Deactivation (true → false):
--     - Stamps cancelled_date = now()
--     - Clears next_charge_date and next_invoice_date
--
--   Reactivation (false → true):
--     - Clears cancelled_date
--     - Walks forward from start_charge_date by the frequency interval until
--       the result is strictly after today — the same loop used in
--       calendar.handle_recurring_event_active_change
--     - Offsets next_invoice_date by the same delta as next_charge_date so
--       charge and invoice dates stay in sync
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION finance.handle_recurring_purchase_active_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  period    INTERVAL;
  next_date DATE;
BEGIN
  -- Deactivation
  IF OLD.active = TRUE AND NEW.active = FALSE THEN
    NEW.cancelled_date    := now();
    NEW.next_charge_date  := NULL;
    NEW.next_invoice_date := NULL;
    RETURN NEW;
  END IF;

  -- Reactivation
  IF OLD.active = FALSE AND NEW.active = TRUE THEN
    NEW.cancelled_date := NULL;

    CASE lower(NEW.frequency)
      WHEN 'daily'     THEN period := INTERVAL '1 day';
      WHEN 'weekly'    THEN period := INTERVAL '1 week';
      WHEN 'biweekly'  THEN period := INTERVAL '2 weeks';
      WHEN 'monthly'   THEN period := INTERVAL '1 month';
      WHEN 'quarterly' THEN period := INTERVAL '3 months';
      WHEN 'yearly'    THEN period := INTERVAL '1 year';
      ELSE RAISE EXCEPTION 'Invalid frequency: %', NEW.frequency;
    END CASE;

    -- Walk forward from start_charge_date to find the next future date
    next_date := NEW.start_charge_date;
    WHILE next_date <= CURRENT_DATE LOOP
      next_date := next_date + period;
    END LOOP;

    NEW.next_charge_date  := next_date;
    -- Keep invoice date offset in sync with charge date offset
    NEW.next_invoice_date := CASE
      WHEN NEW.start_invoice_date IS NOT NULL
      THEN NEW.start_invoice_date + (next_date - NEW.start_charge_date)
      ELSE NULL
    END;
  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- finance.generate_todays_charges()
--
-- Called by cron (daily). Loops all active recurring_purchases whose
-- next_charge_date has arrived, materialises a recurring_purchase_charges row
-- (idempotent — skips if a charge for that date already exists), then rolls
-- next_charge_date (and next_invoice_date) forward by the frequency interval.
--
-- Mirrors calendar.process_recurring_events() in structure.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION finance.generate_todays_charges()
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r      RECORD;
  period INTERVAL;
BEGIN
  FOR r IN
    SELECT *
    FROM finance.recurring_purchases
    WHERE active = TRUE
      AND next_charge_date <= CURRENT_DATE
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

    -- Idempotent insert: skip if a charge for this date already exists
    IF NOT EXISTS (
      SELECT 1
      FROM finance.recurring_purchase_charges c
      WHERE c.recurring_purchase_id = r.id
        AND c.charge_date = r.next_charge_date
    ) THEN
      INSERT INTO finance.recurring_purchase_charges (
        recurring_purchase_id, vendor, item_name,
        charge_date, invoice_date,
        base_cost, tax_cost, other_cost, notes
      ) VALUES (
        r.id, r.vendor, r.item_name,
        r.next_charge_date, r.next_invoice_date,
        r.base_cost, r.tax_cost, r.other_cost, r.notes
      );
    END IF;

    -- Roll schedule forward atomically
    UPDATE finance.recurring_purchases
    SET
      next_charge_date  = r.next_charge_date + period,
      next_invoice_date = CASE
                            WHEN r.next_invoice_date IS NOT NULL
                            THEN r.next_invoice_date + period
                            ELSE NULL
                          END,
      updated_at        = now()
    WHERE id = r.id;

  END LOOP;
END;
$$;

-- -----------------------------------------------------------------------------
-- finance.attach_timestamp_triggers()
--
-- DDL event trigger: automatically attaches created_at / updated_at triggers
-- to any new table created in this schema.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION finance.attach_timestamp_triggers()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
  obj            RECORD;
  has_created_at boolean;
  has_updated_at boolean;
  t_name         text;
  s_name         text;
  trig_exists    boolean;
BEGIN
  FOR obj IN
    SELECT * FROM pg_event_trigger_ddl_commands()
    WHERE command_tag = 'CREATE TABLE'
  LOOP
    s_name := (SELECT n.nspname FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE c.oid = obj.objid);
    t_name := (SELECT c.relname FROM pg_class c WHERE c.oid = obj.objid);

    IF s_name != 'finance' THEN CONTINUE; END IF;

    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = s_name AND table_name = t_name
        AND column_name = 'created_at'
    ) INTO has_created_at;

    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = s_name AND table_name = t_name
        AND column_name = 'updated_at'
    ) INTO has_updated_at;

    IF has_created_at THEN
      SELECT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname  = 'trg_force_created_at'
          AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass
      ) INTO trig_exists;

      IF NOT trig_exists THEN
        EXECUTE format(
          'CREATE TRIGGER trg_force_created_at
           BEFORE INSERT ON %I.%I
           FOR EACH ROW EXECUTE FUNCTION finance.set_created_at();',
          s_name, t_name
        );
      END IF;
    END IF;

    IF has_updated_at THEN
      SELECT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname  = format('trg_%s_updated_at', t_name)
          AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass
      ) INTO trig_exists;

      IF NOT trig_exists THEN
        EXECUTE format(
          'CREATE TRIGGER trg_%I_updated_at
           BEFORE UPDATE ON %I.%I
           FOR EACH ROW EXECUTE FUNCTION finance.set_updated_at();',
          t_name, s_name, t_name
        );
      END IF;
    END IF;
  END LOOP;
END;
$$;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- purchases
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON finance.purchases
  FOR EACH ROW EXECUTE FUNCTION finance.set_created_at();

CREATE TRIGGER trg_purchases_updated_at
  BEFORE UPDATE ON finance.purchases
  FOR EACH ROW EXECUTE FUNCTION finance.set_updated_at();

-- Cross-schema: buying a food/household item auto-updates health inventory
CREATE TRIGGER trg_add_inventory_on_purchase
  AFTER INSERT ON finance.purchases
  FOR EACH ROW EXECUTE FUNCTION health.add_inventory_on_purchase();

-- recurring_purchases
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON finance.recurring_purchases
  FOR EACH ROW EXECUTE FUNCTION finance.set_created_at();

CREATE TRIGGER trg_recurring_purchases_updated_at
  BEFORE UPDATE ON finance.recurring_purchases
  FOR EACH ROW EXECUTE FUNCTION finance.set_updated_at();

-- Normalise frequency to lowercase before CHECK constraint evaluates
CREATE TRIGGER trg_normalize_frequency_lower
  BEFORE INSERT OR UPDATE ON finance.recurring_purchases
  FOR EACH ROW EXECUTE FUNCTION finance.normalize_frequency_lower();

-- Manage active ↔ inactive state: stamp/clear cancelled_date, roll schedule
CREATE TRIGGER trg_recurring_purchases_active_change
  BEFORE UPDATE ON finance.recurring_purchases
  FOR EACH ROW EXECUTE FUNCTION finance.handle_recurring_purchase_active_change();

-- recurring_purchase_charges
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON finance.recurring_purchase_charges
  FOR EACH ROW EXECUTE FUNCTION finance.set_created_at();

CREATE TRIGGER trg_recurring_purchase_charges_updated_at
  BEFORE UPDATE ON finance.recurring_purchase_charges
  FOR EACH ROW EXECUTE FUNCTION finance.set_updated_at();

-- =============================================================================
-- EVENT TRIGGER
-- Automatically attaches timestamp triggers to any new table in this schema
-- =============================================================================

CREATE EVENT TRIGGER finance_timestamp_on_create
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION finance.attach_timestamp_triggers();
