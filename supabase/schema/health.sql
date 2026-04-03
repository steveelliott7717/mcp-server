-- =============================================================================
-- Schema: health (subset)
-- Description: Personal health tracking system. This file covers the core
--              nutrition and fitness pipelines:
--
--   Nutrition pipeline:
--     food_items → recipe_ingredients → recipes → meal_logs
--                                                      ↓
--                                         daily_overview_snapshot
--
--   Fitness pipeline:
--     workout_logs (strength metrics auto-calculated per set)
--     step_logs    (calories calculated with 2-day lag for accuracy)
--
--   Key design patterns:
--   - Macro totals on recipes are maintained automatically by trigger
--     (no denormalization drift possible)
--   - Logging a meal auto-fills all macros from the source recipe/food_item
--   - Daily snapshot is upserted by cron; consumption totals updated live
--     on each meal insert
--   - Strength calorie estimation uses exercise metadata (movement class,
--     equipment, ROM), RPE-based effort, Epley %1RM, and rolling bodyweight
--   - Step calories use a 2-day lag because wearable data often arrives late
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS health;

-- =============================================================================
-- TABLE: health.food_items
-- Nutritional database. Stores full macro + micronutrient label data per item.
-- serving_amount / serving_unit define the reference quantity for all values.
-- recipe_ingredients references this table and scales by (quantity / serving_amount).
-- =============================================================================

CREATE TABLE health.food_items (
  id                    bigint      NOT NULL,
  name                  text        NOT NULL,
  brand                 text,
  category              text,
  category_sub          text,
  product_name          text,

  -- Serving reference
  serving_amount        numeric,        -- e.g. 100 (grams)
  serving_unit          text,           -- e.g. 'g'
  package_size          text,
  container_size        text,
  containers_per_package integer,
  servings_per_container numeric,

  -- Core macros (per serving)
  calories              numeric,
  protein               numeric,
  fat                   numeric,
  carbs                 numeric,
  fiber                 numeric,
  sugar                 numeric,
  added_sugar_g         numeric,

  -- Fat breakdown
  saturated_fat_g       numeric,
  trans_fat_g           numeric,
  monounsaturated_fat_g numeric,
  polyunsaturated_fat_g numeric,
  omega_3_g             numeric,
  omega_6_g             numeric,

  -- Minerals
  sodium_mg             numeric,
  potassium_mg          numeric,
  calcium_mg            numeric,
  iron_mg               numeric,
  magnesium_mg          numeric,
  phosphorus_mg         numeric,
  zinc_mg               numeric,
  copper_mg             numeric,
  manganese_mg          numeric,
  cholesterol_mg        numeric,

  -- Vitamins
  vitamin_a_mcg         numeric,
  vitamin_c_mg          numeric,
  vitamin_d_mcg         numeric,
  vitamin_e_mg          numeric,
  vitamin_k_mcg         numeric,
  thiamin_mg            numeric,
  riboflavin_mg         numeric,
  niacin_mg             numeric,
  vitamin_b6_mg         numeric,
  folate_mcg            numeric,
  vitamin_b12_mcg       numeric,
  pantothenic_acid_mg   numeric,
  choline_mg            numeric,
  biotin_mcg            numeric,
  caffeine_mg           numeric,

  -- Storage
  storage_location_unopened  text,
  storage_location_opened    text,
  shelf_life_unopened_days   integer,
  shelf_life_opened_days     integer,
  typical_store              text,
  notes                      text,

  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT food_items_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: health.recipes
-- Recipe totals (total_calories, protein_g, fat_g, carbs_g) are computed
-- automatically by the update_recipe_nutrition trigger — never set manually.
-- =============================================================================

CREATE TABLE health.recipes (
  id              bigint      NOT NULL DEFAULT nextval('health.recipes_id_seq'),
  name            text        NOT NULL,
  category        text,
  steps           text,

  -- Auto-maintained by update_recipe_nutrition trigger
  total_calories  numeric,
  protein_g       numeric,
  fat_g           numeric,
  carbs_g         numeric,

  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now(),

  CONSTRAINT recipes_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: health.recipe_ingredients
-- Join table between recipes and food_items. quantity is in serving_unit of
-- the food_item. Any change here triggers update_recipe_nutrition to
-- recalculate the parent recipe's macro totals.
-- =============================================================================

CREATE TABLE health.recipe_ingredients (
  id            bigint  NOT NULL DEFAULT nextval('health.recipe_ingredients_id_seq'),
  recipe_id     bigint  NOT NULL,
  food_item_id  bigint  NOT NULL,
  quantity      numeric NOT NULL,
  unit          text    DEFAULT 'g',

  CONSTRAINT recipe_ingredients_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: health.meal_logs
-- Log of meals consumed. One of recipe_id, food_item_id, or batch_prep_id
-- must be set. Macros are auto-filled by f_fill_meal_macros trigger, scaled
-- by quantity. Each insert fires f_update_daily_overview_consumption to
-- update the running totals on daily_overview_snapshot.
-- =============================================================================

CREATE TABLE health.meal_logs (
  id            bigint  NOT NULL DEFAULT nextval('health.meal_logs_id_seq'),
  recipe_id     bigint,
  food_item_id  bigint,
  batch_prep_id bigint,
  quantity      numeric DEFAULT 1,

  -- Auto-filled by f_fill_meal_macros trigger (base macros × quantity)
  calories      numeric NOT NULL,
  protein_g     numeric,
  fat_g         numeric,
  carbs_g       numeric,

  notes         text,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now(),

  CONSTRAINT meal_logs_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: health.daily_overview_snapshot
-- One row per day. Populated by rpc_snapshot_daily_overview() (cron).
-- Consumption columns (calories_logged etc.) are updated live by the
-- f_update_daily_overview_consumption trigger on meal_logs inserts.
--
-- Target calculation (in rpc_snapshot_daily_overview):
--   BMR via Mifflin-St Jeor using 3-day rolling avg bodyweight
--   + activity_kcal (step calories with 2-day lag + today's workouts)
--   + goal_adjustment_kcal (from current weekly_programs row)
--   = total_kcal_target
-- =============================================================================

CREATE TABLE health.daily_overview_snapshot (
  id                    bigint  NOT NULL DEFAULT nextval('health.daily_overview_snapshot_id_seq'),
  snapshot_date         date,

  -- Calorie targets (set by cron snapshot)
  bmr_kcal              numeric,
  activity_kcal         numeric,
  goal_adjustment_kcal  integer,
  total_kcal_target     numeric,

  -- Macro targets
  protein_g             numeric,
  fat_g                 numeric,
  carbs_g               numeric,

  -- Weight trends (from v_weight_trends view)
  avg_3day              numeric,
  avg_7day              numeric,
  weekly_gain_lbs       numeric,

  -- Consumption running totals (updated live on each meal_logs insert)
  calories_logged       numeric  DEFAULT 0,
  protein_logged_g      numeric  DEFAULT 0,
  fat_logged_g          numeric  DEFAULT 0,
  carbs_logged_g        numeric  DEFAULT 0,

  created_at            timestamptz DEFAULT now(),

  CONSTRAINT daily_overview_snapshot_pkey PRIMARY KEY (id),
  CONSTRAINT daily_overview_snapshot_snapshot_date_key UNIQUE (snapshot_date)
);

-- =============================================================================
-- TABLE: health.workout_logs
-- One row per set. calories_burned and total_volume are auto-calculated
-- by health_calc_strength_metrics trigger using exercise metadata, RPE,
-- Epley %1RM estimation, and rolling bodyweight.
-- =============================================================================

CREATE TABLE health.workout_logs (
  id                  bigint  NOT NULL DEFAULT nextval('health.workout_logs_id_seq'),
  workout_name        text    NOT NULL,
  exercise_id         bigint,
  scheduled_date      date,
  exercise_order      integer,
  category            text,

  -- Performance
  sets_completed      integer,
  reps                integer  DEFAULT 0,
  weight_lbs          numeric,
  rpe                 numeric,    -- Rate of Perceived Exertion (0–10)
  set_duration_seconds numeric,
  rest_seconds        numeric,

  -- Auto-calculated by health_calc_strength_metrics trigger
  total_volume        numeric,    -- sets × reps × weight_lbs
  calories_burned     numeric,

  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT workout_logs_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: health.step_logs
-- Daily step counts. calories_burned is calculated with a 2-day lag because
-- wearable/phone data often arrives late. On INSERT, actual_steps_taken is
-- set to NULL and calories_burned to 0. A separate trigger/cron fills in
-- actual_steps_taken two days later and recalculates calories.
-- =============================================================================

CREATE TABLE health.step_logs (
  id                  bigint  NOT NULL DEFAULT nextval('health.step_logs_id_seq'),
  date                date,
  steps               integer NOT NULL,   -- estimated / target steps
  steps_agg           integer,            -- aggregated from wearable
  actual_steps_taken  integer,            -- filled in after 2-day lag
  calories_burned     numeric,            -- steps × bodyweight_kg × 0.00055

  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT step_logs_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: health.weight_logs
-- Daily weigh-ins. Used by v_weight_trends view (3-day and 7-day rolling
-- averages) which is referenced by both the strength calorie formula and
-- the daily snapshot BMR calculation.
-- =============================================================================

CREATE TABLE health.weight_logs (
  id          bigint  NOT NULL DEFAULT nextval('health.weight_logs_id_seq'),
  weight_lbs  numeric NOT NULL,
  log_date    date,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT weight_logs_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION health.set_created_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.created_at := now(); RETURN NEW; END;
$$;

CREATE OR REPLACE FUNCTION health.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END;
$$;

-- -----------------------------------------------------------------------------
-- health.update_recipe_nutrition()
--
-- AFTER INSERT/UPDATE/DELETE on recipe_ingredients. Recomputes total_calories,
-- protein_g, fat_g, carbs_g on the parent recipes row by joining all
-- ingredients to food_items and scaling by (quantity / serving_amount).
-- Handles the empty-recipe case (zeroes out totals rather than leaving stale).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.update_recipe_nutrition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  target_recipe_id bigint;
BEGIN
  target_recipe_id := CASE TG_OP WHEN 'DELETE' THEN OLD.recipe_id ELSE NEW.recipe_id END;

  IF target_recipe_id IS NULL THEN RETURN NULL; END IF;

  UPDATE health.recipes r
  SET
    total_calories = COALESCE(sub.calories, 0),
    protein_g      = COALESCE(sub.protein,  0),
    fat_g          = COALESCE(sub.fat,      0),
    carbs_g        = COALESCE(sub.carbs,    0),
    updated_at     = now()
  FROM (
    SELECT
      ri.recipe_id,
      SUM(COALESCE(fi.calories, 0) * (ri.quantity / COALESCE(NULLIF(fi.serving_amount, 0), 100))) AS calories,
      SUM(COALESCE(fi.protein,  0) * (ri.quantity / COALESCE(NULLIF(fi.serving_amount, 0), 100))) AS protein,
      SUM(COALESCE(fi.fat,      0) * (ri.quantity / COALESCE(NULLIF(fi.serving_amount, 0), 100))) AS fat,
      SUM(COALESCE(fi.carbs,    0) * (ri.quantity / COALESCE(NULLIF(fi.serving_amount, 0), 100))) AS carbs
    FROM health.recipe_ingredients ri
    LEFT JOIN health.food_items fi ON fi.id = ri.food_item_id
    WHERE ri.recipe_id = target_recipe_id
    GROUP BY ri.recipe_id
  ) sub
  WHERE r.id = target_recipe_id;

  -- Zero out if recipe now has no ingredients
  IF NOT FOUND THEN
    UPDATE health.recipes
    SET total_calories = 0, protein_g = 0, fat_g = 0, carbs_g = 0, updated_at = now()
    WHERE id = target_recipe_id;
  END IF;

  RETURN COALESCE(NEW, OLD);
END;
$$;

-- -----------------------------------------------------------------------------
-- health.f_fill_meal_macros()
--
-- BEFORE INSERT/UPDATE on meal_logs. Looks up macros from the linked source
-- (recipe, food_item, or batch_prep), scales by quantity, and writes them
-- into the row — no manual macro entry required.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.f_fill_meal_macros()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  base_calories numeric;
  base_protein  numeric;
  base_fat      numeric;
  base_carbs    numeric;
BEGIN
  IF NEW.recipe_id IS NULL AND NEW.food_item_id IS NULL AND NEW.batch_prep_id IS NULL THEN
    RAISE NOTICE 'No source provided; skipping macro fill.';
    RETURN NEW;
  END IF;

  IF NEW.recipe_id IS NOT NULL THEN
    SELECT total_calories, protein_g, fat_g, carbs_g
      INTO base_calories, base_protein, base_fat, base_carbs
      FROM health.recipes WHERE id = NEW.recipe_id;

  ELSIF NEW.food_item_id IS NOT NULL THEN
    SELECT calories, protein, fat, carbs
      INTO base_calories, base_protein, base_fat, base_carbs
      FROM health.food_items WHERE id = NEW.food_item_id;

  ELSIF NEW.batch_prep_id IS NOT NULL THEN
    SELECT total_calories, protein_g, fat_g, carbs_g
      INTO base_calories, base_protein, base_fat, base_carbs
      FROM health.batch_prep WHERE id = NEW.batch_prep_id;
  END IF;

  IF base_calories IS NOT NULL THEN
    NEW.calories  := ROUND(base_calories * COALESCE(NEW.quantity, 1), 1);
    NEW.protein_g := ROUND(base_protein  * COALESCE(NEW.quantity, 1), 1);
    NEW.fat_g     := ROUND(base_fat      * COALESCE(NEW.quantity, 1), 1);
    NEW.carbs_g   := ROUND(base_carbs    * COALESCE(NEW.quantity, 1), 1);
  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- health.f_update_daily_overview_consumption()
--
-- AFTER INSERT on meal_logs. Aggregates all meal totals for the local CST date
-- and upserts them onto daily_overview_snapshot. Keeps the snapshot's
-- consumption columns accurate in real time without a scheduled job.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.f_update_daily_overview_consumption()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO 'health', 'pg_temp' AS $$
DECLARE
  v_date   date;
  v_cal    numeric;
  v_protein numeric;
  v_fat    numeric;
  v_carbs  numeric;
BEGIN
  v_date := (NEW.created_at AT TIME ZONE 'America/Chicago')::date;

  SELECT
    COALESCE(SUM(calories), 0),
    COALESCE(SUM(protein_g), 0),
    COALESCE(SUM(fat_g), 0),
    COALESCE(SUM(carbs_g), 0)
  INTO v_cal, v_protein, v_fat, v_carbs
  FROM health.meal_logs
  WHERE (created_at AT TIME ZONE 'America/Chicago')::date = v_date;

  UPDATE health.daily_overview_snapshot
  SET calories_logged  = v_cal,
      protein_logged_g = v_protein,
      fat_logged_g     = v_fat,
      carbs_logged_g   = v_carbs
  WHERE snapshot_date = v_date;

  IF NOT FOUND THEN
    INSERT INTO health.daily_overview_snapshot
      (snapshot_date, calories_logged, protein_logged_g, fat_logged_g, carbs_logged_g)
    VALUES (v_date, v_cal, v_protein, v_fat, v_carbs);
  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- health.health_calc_strength_metrics()
--
-- BEFORE INSERT/UPDATE on workout_logs. Calculates two values:
--
--   total_volume = sets × reps × weight_lbs
--
--   calories_burned uses a multi-factor model:
--     - effort_factor:    exponential RPE curve (0.07–0.11)
--     - movement_mult:    compound/isolation × movement type × equipment
--     - time_factor:      logistic scaling on set duration
--     - rest_factor:      exponential decay with longer rest periods
--     - intensity_factor: continuous Epley %1RM → load intensity
--     - muscle_factor:    primary muscle class + secondary muscle count
--     - rom_factor:       full/partial/variable range of motion
--     - rest_calories:    METS-based metabolism during rest periods
--
-- Bodyweight is sourced from the v_weight_trends view (3-day rolling avg).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.health_calc_strength_metrics()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  user_weight_kg  numeric(6,2);
  effort_factor   numeric(4,2);
  movement_mult   numeric(4,2) := 1.0;
  time_factor     numeric(4,2) := 1.0;
  rest_factor     numeric(4,2) := 1.0;
  intensity_factor numeric(4,2) := 1.0;
  muscle_factor   numeric(4,2) := 1.0;
  rom_factor      numeric(4,2) := 1.0;
  ex_class        text;
  ex_type         text;
  ex_equip        text;
  rom             text;
  p_muscle        text;
  s_muscles       text[];
  percent_1rm     numeric(5,2);
  rest_calories   numeric(6,1);
BEGIN
  -- Total lifted volume
  NEW.total_volume :=
    COALESCE(NEW.sets_completed, 1)
    * COALESCE(NEW.reps, 0)
    * COALESCE(NEW.weight_lbs, 0);

  IF NEW.total_volume = 0 THEN
    NEW.calories_burned := 0;
    NEW.updated_at := now();
    RETURN NEW;
  END IF;

  -- 3-day rolling average bodyweight (lbs → kg)
  SELECT COALESCE(avg_3day * 0.453592, 70.0)
    INTO user_weight_kg
    FROM health.v_weight_trends
    WHERE log_date = (SELECT MAX(log_date) FROM health.v_weight_trends WHERE avg_3day IS NOT NULL);

  IF user_weight_kg IS NULL OR user_weight_kg <= 0 THEN
    user_weight_kg := 70.0;
  END IF;

  -- Exponential RPE effort factor (0.07 at RPE 0 → 0.11 at RPE 10)
  IF NEW.rpe IS NULL THEN
    effort_factor := 0.08;
  ELSE
    DECLARE rpe_clamped numeric := LEAST(GREATEST(NEW.rpe, 0), 10); BEGIN
      effort_factor := 0.07 + POWER(rpe_clamped / 10.0, 1.6) * 0.04;
    END;
  END IF;

  -- Exercise metadata
  SELECT movement_class, movement_type, equipment, primary_muscle, secondary_muscles, range_of_motion
    INTO ex_class, ex_type, ex_equip, p_muscle, s_muscles, rom
    FROM health.exercises WHERE id = NEW.exercise_id;

  -- Movement multipliers (compound/isolation × type × equipment)
  movement_mult :=
    CASE ex_class WHEN 'Compound' THEN 1.3 WHEN 'Isolation' THEN 0.8 WHEN 'Core' THEN 0.9 ELSE 1.0 END
    * CASE ex_type WHEN 'Squat' THEN 1.5 WHEN 'Hinge' THEN 1.4 WHEN 'Pull' THEN 1.1 WHEN 'Push' THEN 1.0 ELSE 1.0 END
    * CASE ex_equip WHEN 'Barbell' THEN 1.1 WHEN 'Machine' THEN 0.85 ELSE 1.0 END;

  -- Set duration factor (logistic, base 1.0 at 30s, +40% at 90s)
  IF NEW.set_duration_seconds IS NOT NULL THEN
    DECLARE dur numeric := LEAST(GREATEST(NEW.set_duration_seconds, 5), 120); BEGIN
      time_factor := 1.0 + 0.4 * (1 / (1 + EXP(-0.08 * (dur - 60))) - 0.5) * 2;
    END;
  END IF;

  -- Rest period factor (exponential decay: 1.15 at 30s → 0.9 at 240s)
  IF NEW.rest_seconds IS NOT NULL THEN
    DECLARE rest numeric := LEAST(GREATEST(NEW.rest_seconds, 15), 300); BEGIN
      rest_factor := 0.9 + 0.25 * EXP(-0.01 * rest);
    END;
  END IF;

  -- Epley %1RM → intensity factor (continuous, 50%→0.7, 100%→1.3)
  percent_1rm := CASE WHEN NEW.reps IS NULL OR NEW.reps <= 0 THEN 70
                      ELSE 100 / (1 + 0.0333 * NEW.reps) END;
  percent_1rm := LEAST(GREATEST(percent_1rm, 50), 100);
  intensity_factor := 0.7 + POWER((percent_1rm - 50) / 50.0, 1.3) * 0.6;

  -- Muscle engagement (compound bonus + secondary muscle count)
  muscle_factor := GREATEST(
    1.0
    + CASE ex_class WHEN 'Compound' THEN 0.07 ELSE 0.05 END
    + COALESCE(array_length(s_muscles, 1), 0) * 0.025,
    1.05
  );

  -- Range-of-motion multiplier
  rom_factor := CASE rom WHEN 'full' THEN 1.1 WHEN 'partial' THEN 0.9 ELSE 1.0 END;

  -- Rest period metabolism (METs-based)
  rest_calories :=
    (COALESCE(NEW.rest_seconds, 90) * COALESCE(NEW.sets_completed, 1) / 60.0)
    * 2.5 * user_weight_kg / 60.0;

  -- Final: mechanical work formula + rest metabolism
  NEW.calories_burned :=
    ROUND(
      (NEW.total_volume / 1000.0)
      * effort_factor * user_weight_kg
      * movement_mult * time_factor * rest_factor
      * intensity_factor * muscle_factor * rom_factor,
    1) + ROUND(rest_calories, 1);

  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- health.auto_calc_step_calories()
--
-- BEFORE INSERT/UPDATE on step_logs. On INSERT, clears actual_steps_taken
-- (filled later by the 2-day lag job) and zeros calories_burned. On UPDATE,
-- recalculates calories using actual_steps_taken × bodyweight × 0.00055.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.auto_calc_step_calories()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.actual_steps_taken := NULL;
    NEW.calories_burned    := 0;
  ELSIF TG_OP = 'UPDATE' THEN
    NEW.calories_burned :=
      ROUND(
        COALESCE(NEW.actual_steps_taken, 0)
        * (SELECT COALESCE(avg_3day * 0.453592, 70.0) FROM health.v_weight_trends ORDER BY log_date DESC LIMIT 1)
        * 0.00055,
      1);
  END IF;
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- health.rpc_snapshot_daily_overview()
--
-- Called by cron daily. Calculates today's calorie targets via a CTE chain:
--   1. BMR using Mifflin-St Jeor with 3-day rolling avg bodyweight
--   2. Activity: step calories (2-day lag) + today's strength + bike calories
--   3. Goal adjustment from the most recent weekly_programs row
--   4. Macro targets: protein = 1g/lb, fat = 0.4g/lb, carbs = remainder
--   5. Weight trend deltas (7-day rolling avg vs 7 days prior)
--
-- Upserts on snapshot_date so it's safe to call multiple times per day.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION health.rpc_snapshot_daily_overview()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  WITH
  avg_weight AS (
    SELECT avg_3day AS weight_lbs FROM health.v_weight_trends ORDER BY log_date DESC LIMIT 1
  ),
  bmr AS (
    SELECT
      ROUND(
        10 * (aw.weight_lbs * 0.4536)
        + 6.25 * (up.height_in * 2.54)
        - 5 * EXTRACT(YEAR FROM AGE(up.date_of_birth))
        + CASE WHEN up.sex = 'male' THEN 5 ELSE -161 END,
      0) AS bmr_kcal,
      aw.weight_lbs
    FROM professional_profile.user_profile up
    CROSS JOIN avg_weight aw
    LIMIT 1
  ),
  step_kcal AS (   -- 2-day lag: wearable data arrives late
    SELECT COALESCE(SUM(calories_burned), 0) AS step_kcal
    FROM health.step_logs WHERE date = CURRENT_DATE - INTERVAL '2 days'
  ),
  strength_kcal AS (
    SELECT COALESCE(SUM(calories_burned), 0) AS strength_kcal
    FROM health.workout_logs WHERE scheduled_date = CURRENT_DATE
  ),
  bike_kcal AS (
    SELECT COALESCE(SUM(calories_burned), 0) AS bike_kcal
    FROM health.bike_logs WHERE scheduled_date = CURRENT_DATE
  ),
  goal AS (
    SELECT COALESCE(kcal_adjustment, 0) AS goal_adjustment_kcal
    FROM health.weekly_programs ORDER BY start_date DESC LIMIT 1
  ),
  weight_trend AS (
    SELECT
      COALESCE(avg_3day, 0) AS avg_3day,
      COALESCE(avg_7day, 0) AS avg_7day,
      COALESCE(ROUND(avg_7day - (
        SELECT avg_7day FROM health.v_weight_trends
        WHERE log_date <= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY log_date DESC LIMIT 1
      ), 2), 0) AS weekly_gain_lbs
    FROM health.v_weight_trends ORDER BY log_date DESC LIMIT 1
  ),
  calc AS (
    SELECT
      b.bmr_kcal,
      s.step_kcal + st.strength_kcal + bk.bike_kcal AS activity_kcal,
      g.goal_adjustment_kcal,
      b.bmr_kcal + s.step_kcal + st.strength_kcal + bk.bike_kcal + g.goal_adjustment_kcal AS total_kcal_target,
      ROUND(b.weight_lbs * 1.0, 0)  AS protein_g,   -- 1g per lb bodyweight
      ROUND(b.weight_lbs * 0.4, 0)  AS fat_g,       -- 0.4g per lb bodyweight
      ROUND((
        b.bmr_kcal + s.step_kcal + st.strength_kcal + bk.bike_kcal + g.goal_adjustment_kcal
        - (b.weight_lbs * 4 + b.weight_lbs * 0.3 * 9)  -- protein + fat calories
      ) / 4, 0) AS carbs_g,
      wt.avg_3day, wt.avg_7day, wt.weekly_gain_lbs
    FROM bmr b
    LEFT JOIN step_kcal s ON TRUE
    LEFT JOIN strength_kcal st ON TRUE
    LEFT JOIN bike_kcal bk ON TRUE
    LEFT JOIN goal g ON TRUE
    LEFT JOIN weight_trend wt ON TRUE
  )
  INSERT INTO health.daily_overview_snapshot (
    snapshot_date, bmr_kcal, activity_kcal, goal_adjustment_kcal, total_kcal_target,
    protein_g, fat_g, carbs_g, avg_3day, avg_7day, weekly_gain_lbs, created_at
  )
  SELECT CURRENT_DATE, bmr_kcal, activity_kcal, goal_adjustment_kcal, total_kcal_target,
    protein_g, fat_g, carbs_g, avg_3day, avg_7day, weekly_gain_lbs, now()
  FROM calc
  ON CONFLICT (snapshot_date) DO UPDATE SET
    bmr_kcal             = EXCLUDED.bmr_kcal,
    activity_kcal        = EXCLUDED.activity_kcal,
    goal_adjustment_kcal = EXCLUDED.goal_adjustment_kcal,
    total_kcal_target    = EXCLUDED.total_kcal_target,
    protein_g            = EXCLUDED.protein_g,
    fat_g                = EXCLUDED.fat_g,
    carbs_g              = EXCLUDED.carbs_g,
    avg_3day             = EXCLUDED.avg_3day,
    avg_7day             = EXCLUDED.avg_7day,
    weekly_gain_lbs      = EXCLUDED.weekly_gain_lbs,
    created_at           = now();
END;
$$;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- recipe_ingredients: recalculate parent recipe totals on any change
CREATE TRIGGER trg_update_recipe_nutrition
  AFTER INSERT OR UPDATE OR DELETE ON health.recipe_ingredients
  FOR EACH ROW EXECUTE FUNCTION health.update_recipe_nutrition();

-- meal_logs: auto-fill macros before insert
CREATE TRIGGER trg_fill_meal_macros
  BEFORE INSERT OR UPDATE ON health.meal_logs
  FOR EACH ROW EXECUTE FUNCTION health.f_fill_meal_macros();

-- meal_logs: update daily snapshot consumption totals after insert
CREATE TRIGGER trg_update_overview_consumption
  AFTER INSERT ON health.meal_logs
  FOR EACH ROW EXECUTE FUNCTION health.f_update_daily_overview_consumption();

-- workout_logs: calculate volume and calories before insert/update
CREATE TRIGGER trg_strength_metrics
  BEFORE INSERT OR UPDATE ON health.workout_logs
  FOR EACH ROW EXECUTE FUNCTION health.health_calc_strength_metrics();

-- step_logs: set up on insert, recalculate calories on update
CREATE TRIGGER trg_auto_calc_steps_insert
  BEFORE INSERT ON health.step_logs
  FOR EACH ROW EXECUTE FUNCTION health.auto_calc_step_calories();

CREATE TRIGGER trg_auto_calc_steps_update
  BEFORE UPDATE ON health.step_logs
  FOR EACH ROW EXECUTE FUNCTION health.auto_calc_step_calories();

-- Standard timestamp triggers
CREATE TRIGGER trg_force_created_at BEFORE INSERT ON health.recipes FOR EACH ROW EXECUTE FUNCTION health.set_created_at();
CREATE TRIGGER trg_recipes_updated_at BEFORE UPDATE ON health.recipes FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();
CREATE TRIGGER trg_force_created_at BEFORE INSERT ON health.meal_logs FOR EACH ROW EXECUTE FUNCTION health.set_created_at();
CREATE TRIGGER trg_meal_logs_updated_at BEFORE UPDATE ON health.meal_logs FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();
CREATE TRIGGER trg_force_created_at BEFORE INSERT ON health.workout_logs FOR EACH ROW EXECUTE FUNCTION health.set_created_at();
CREATE TRIGGER trg_workout_logs_updated_at BEFORE UPDATE ON health.workout_logs FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();
CREATE TRIGGER trg_force_created_at BEFORE INSERT ON health.step_logs FOR EACH ROW EXECUTE FUNCTION health.set_created_at();
CREATE TRIGGER trg_step_logs_updated_at BEFORE UPDATE ON health.step_logs FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();
CREATE TRIGGER trg_force_created_at BEFORE INSERT ON health.weight_logs FOR EACH ROW EXECUTE FUNCTION health.set_created_at();
CREATE TRIGGER trg_weight_logs_updated_at BEFORE UPDATE ON health.weight_logs FOR EACH ROW EXECUTE FUNCTION health.set_updated_at();

-- =============================================================================
-- CRON
-- rpc_snapshot_daily_overview() is called once per day to calculate targets.
-- Example pg_cron registration:
--
--   SELECT cron.schedule('snapshot-daily-overview', '30 6 * * *',
--     'SELECT health.rpc_snapshot_daily_overview()');
-- =============================================================================
