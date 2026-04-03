-- =============================================================================
-- Schema: genealogy
-- Description: Family tree database with document procurement tracking.
--              Persons form a self-referential graph (father/mother/spouse FKs).
--              Documents track a two-track workflow: certified copy + apostille,
--              and separately, translation. Trigger automation keeps denormalized
--              name fields consistent and auto-creates calendar events when
--              document deadlines are set.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS genealogy;

-- =============================================================================
-- TABLE: genealogy.persons
--
-- Self-referential family tree. father_id, mother_id, and spouse_id all point
-- back into this table. Denormalized name fields (father_name etc.) are kept
-- in sync automatically by the fill_names_from_links and propagate_name_changes
-- triggers — no application logic required.
-- =============================================================================

CREATE TABLE genealogy.persons (
  id                  bigint      NOT NULL DEFAULT nextval('genealogy.persons_id_seq'),

  -- Name
  first_name          text        NOT NULL,
  middle_name         text,
  last_name           text        NOT NULL,
  suffix              text,
  previous_first_name text,           -- maiden / prior name
  previous_last_name  text,

  -- Demographics
  gender              text,
  birth_date          date,
  birth_city          text,
  birth_county        text,
  birth_state         text,
  birth_country       text,
  death_date          date,
  death_city          text,
  death_county        text,
  death_state         text,
  death_country       text,
  citizenship_primary   text,
  citizenship_secondary text,
  occupation          text,

  -- Relationships (FK → self; SET NULL on delete to preserve tree integrity)
  father_id           bigint,
  mother_id           bigint,
  spouse_id           bigint,

  -- Denormalized display names — auto-populated by fill_names_from_links trigger
  father_name         text,
  mother_name         text,
  spouse_name         text,
  marriage_date       date,
  marriage_place      text,

  -- Notes
  parentage_notes     text,

  created_at          timestamptz DEFAULT now(),
  updated_at          timestamptz DEFAULT now(),

  CONSTRAINT persons_pkey         PRIMARY KEY (id),
  CONSTRAINT persons_father_id_fkey
    FOREIGN KEY (father_id) REFERENCES genealogy.persons(id) ON DELETE SET NULL,
  CONSTRAINT persons_mother_id_fkey
    FOREIGN KEY (mother_id) REFERENCES genealogy.persons(id) ON DELETE SET NULL,
  CONSTRAINT persons_spouse_id_fkey
    FOREIGN KEY (spouse_id) REFERENCES genealogy.persons(id)
);

-- =============================================================================
-- TABLE: genealogy.documents
--
-- Tracks official records needed for genealogy research and citizenship
-- applications. Two parallel state machines are enforced via CHECK constraints
-- and auto-advanced by the update_next_steps trigger:
--
--   Document workflow:    not_ordered → ordered_certified_copy →
--                         received_certified_copy → sent_for_apostille →
--                         received_apostille → finalized
--
--   Translation workflow: not_requested → requested_translation →
--                         received_translation → finalized
--
-- When a next_step_date or translation_next_step_date is set, the
-- auto_create_document_calendar_events trigger writes a calendar.events row
-- automatically (cross-schema).
-- =============================================================================

CREATE TABLE genealogy.documents (
  id                      bigint      NOT NULL DEFAULT nextval('genealogy.documents_id_seq'),

  -- Identity
  doc_title               text        NOT NULL,
  doc_type                text,
  language                text,
  file_url                text,
  content                 text,           -- full text for embedding
  content_hash            text,           -- md5 of content; cleared on change to trigger re-embed

  -- Issuance
  recording_office_name   text,
  record_date             date,
  issue_date              date,
  event_date              date,
  event_location          text,
  expiry_date             date,

  -- Officials
  record_signatory_name   text,
  record_signatory_title  text,
  record_signatory_office text,
  record_signatory_signed_date date,
  recording_officer_name  text,
  recording_officer_title text,
  issuing_officer_name    text,
  issuing_officer_title   text,
  issuing_authority_name  text,

  -- Embeddings (pgvector)
  embedding               vector,
  embedding_model         text        DEFAULT 'text-embedding-3-large',
  embedded_at             timestamptz,

  -- Document procurement workflow
  status                  text        DEFAULT 'not_ordered',
  status_date             date,
  next_step               text,
  next_step_date          date,

  -- Translation workflow
  translation_status      text        DEFAULT 'not_requested',
  translation_status_date date,
  translation_next_step   text,
  translation_next_step_date date,

  created_at              timestamptz DEFAULT now(),
  updated_at              timestamptz DEFAULT now(),

  CONSTRAINT documents_pkey PRIMARY KEY (id),

  -- Enforce valid document procurement states
  CONSTRAINT check_documents_status CHECK (status = ANY (ARRAY[
    'not_ordered', 'asked_for_certified_copy', 'ordered_certified_copy',
    'received_certified_copy', 'sent_for_apostille', 'received_apostille', 'finalized'
  ])),
  CONSTRAINT check_documents_next_step CHECK (
    next_step IS NULL OR next_step = ANY (ARRAY[
      'order_certified_copy', 'wait_for_certified_copy', 'send_for_apostille',
      'wait_for_apostille', 'finalize'
    ])
  ),

  -- Enforce valid translation states
  CONSTRAINT check_translation_status CHECK (translation_status = ANY (ARRAY[
    'not_requested', 'requested_translation', 'received_translation', 'finalized'
  ])),
  CONSTRAINT check_translation_next_step CHECK (
    translation_next_step IS NULL OR translation_next_step = ANY (ARRAY[
      'request_translation', 'wait_for_translation', 'finalize'
    ])
  )
);

-- =============================================================================
-- TABLE: genealogy.document_person_links
-- Many-to-many between documents and persons.
-- Cascade deletes: removing a document or person cleans up all links.
-- =============================================================================

CREATE TABLE genealogy.document_person_links (
  id          bigint      NOT NULL DEFAULT nextval('genealogy.document_person_links_id_seq'),
  document_id bigint      NOT NULL,
  person_id   bigint      NOT NULL,
  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now(),

  CONSTRAINT document_person_links_pkey PRIMARY KEY (id),
  CONSTRAINT document_person_links_document_id_person_id_key UNIQUE (document_id, person_id),
  CONSTRAINT document_person_links_document_id_fkey
    FOREIGN KEY (document_id) REFERENCES genealogy.documents(id) ON DELETE CASCADE,
  CONSTRAINT document_person_links_person_id_fkey
    FOREIGN KEY (person_id)   REFERENCES genealogy.persons(id)   ON DELETE CASCADE
);

-- =============================================================================
-- TABLE: genealogy.user_profile
-- Personal profile of the user conducting research (used for applications).
-- =============================================================================

CREATE TABLE genealogy.user_profile (
  id                    bigint  NOT NULL,
  first_name            text    NOT NULL,
  middle_name           text,
  last_name             text    NOT NULL,
  preferred_name        text,
  date_of_birth         date,
  sex                   text,
  race                  text,
  marital_status        text,
  language_preference   text,
  place_of_birth        text,
  citizenships          text[],
  height_in             numeric,
  dominant_hand         text,
  primary_email         text,
  secondary_email       text,
  phone_number          text,
  address_line1         text,
  address_line2         text,
  city                  text,
  state_province_region text,
  postal_code           text,
  country               text,
  created_at            timestamptz DEFAULT now(),
  updated_at            timestamptz DEFAULT now(),

  CONSTRAINT user_profile_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

CREATE OR REPLACE FUNCTION genealogy.set_created_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.created_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION genealogy.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- genealogy.fill_names_from_links()
--
-- BEFORE INSERT OR UPDATE on persons. When father_id / mother_id / spouse_id
-- are set, looks up the referenced row and denormalizes the full name into
-- father_name / mother_name / spouse_name. Recursion guard via
-- pg_trigger_depth() prevents infinite loops when propagate_name_changes
-- fires and causes further updates.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION genealogy.fill_names_from_links()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF pg_trigger_depth() > 1 THEN
    RETURN NEW;
  END IF;

  IF NEW.father_id IS NOT NULL THEN
    SELECT first_name || ' ' || COALESCE(middle_name || ' ', '') || last_name || COALESCE(' ' || suffix, '')
      INTO NEW.father_name
      FROM genealogy.persons WHERE id = NEW.father_id;
  ELSE
    NEW.father_name := NULL;
  END IF;

  IF NEW.mother_id IS NOT NULL THEN
    SELECT first_name || ' ' || COALESCE(middle_name || ' ', '') || last_name || COALESCE(' ' || suffix, '')
      INTO NEW.mother_name
      FROM genealogy.persons WHERE id = NEW.mother_id;
  ELSE
    NEW.mother_name := NULL;
  END IF;

  IF NEW.spouse_id IS NOT NULL THEN
    SELECT first_name || ' ' || COALESCE(middle_name || ' ', '') || last_name || COALESCE(' ' || suffix, '')
      INTO NEW.spouse_name
      FROM genealogy.persons WHERE id = NEW.spouse_id;
  ELSE
    NEW.spouse_name := NULL;
  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- genealogy.propagate_name_changes()
--
-- AFTER UPDATE on persons. When a person's own name changes, fans out updates
-- to all rows that reference them as father, mother, or spouse so denormalized
-- name strings stay consistent across the tree. Uses IS DISTINCT FROM to
-- avoid no-op updates; recursion guard prevents loops.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION genealogy.propagate_name_changes()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  fullname text;
BEGIN
  IF pg_trigger_depth() > 1 THEN
    RETURN NEW;
  END IF;

  fullname := NEW.first_name || ' ' || COALESCE(NEW.middle_name || ' ', '') ||
              NEW.last_name || COALESCE(' ' || NEW.suffix, '');

  UPDATE genealogy.persons SET father_name = fullname
    WHERE father_id = NEW.id AND father_name IS DISTINCT FROM fullname;

  UPDATE genealogy.persons SET mother_name = fullname
    WHERE mother_id = NEW.id AND mother_name IS DISTINCT FROM fullname;

  UPDATE genealogy.persons SET spouse_name = fullname
    WHERE spouse_id = NEW.id AND spouse_name IS DISTINCT FROM fullname;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- genealogy.update_next_steps()
--
-- BEFORE INSERT OR UPDATE on documents. Derives next_step from status and
-- translation_next_step from translation_status — the application only needs
-- to set the status field; the workflow logic lives here in the database.
-- Also stamps status_date / translation_status_date whenever the status changes.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION genealogy.update_next_steps()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- Document procurement workflow
  CASE NEW.status
    WHEN 'not_ordered'              THEN NEW.next_step := 'order_certified_copy';
    WHEN 'asked_for_certified_copy' THEN NEW.next_step := 'order_certified_copy';
    WHEN 'ordered_certified_copy'   THEN NEW.next_step := 'wait_for_certified_copy';
    WHEN 'received_certified_copy'  THEN NEW.next_step := 'send_for_apostille';
    WHEN 'sent_for_apostille'       THEN NEW.next_step := 'wait_for_apostille';
    WHEN 'received_apostille'       THEN NEW.next_step := 'finalize';
    WHEN 'finalized'                THEN NEW.next_step := NULL;
    ELSE NEW.next_step := COALESCE(NEW.next_step, 'ordered');
  END CASE;

  IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status THEN
    NEW.status_date := now();
  END IF;

  -- Translation workflow
  CASE NEW.translation_status
    WHEN 'not_requested'        THEN NEW.translation_next_step := 'request_translation';
    WHEN 'requested_translation' THEN NEW.translation_next_step := 'wait_for_translation';
    WHEN 'received_translation' THEN NEW.translation_next_step := 'finalize';
    WHEN 'finalized'            THEN NEW.translation_next_step := NULL;
    ELSE NEW.translation_next_step := COALESCE(NEW.translation_next_step, 'request_translation');
  END CASE;

  IF TG_OP = 'INSERT' OR NEW.translation_status IS DISTINCT FROM OLD.translation_status THEN
    NEW.translation_status_date := now();
  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- genealogy.auto_create_document_calendar_events()
--
-- AFTER UPDATE on documents. Cross-schema trigger: when next_step_date or
-- translation_next_step_date is set (or changed), automatically inserts a
-- calendar.events row at 10 AM on that date. Keeps the calendar in sync with
-- the document workflow without any application-layer coordination.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION genealogy.auto_create_document_calendar_events()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  v_event_title       text;
  v_event_description text;
  v_start_time        timestamptz;
BEGIN
  -- Document workflow deadline
  IF NEW.next_step_date IS DISTINCT FROM OLD.next_step_date
     AND NEW.next_step_date IS NOT NULL THEN

    v_start_time := (NEW.next_step_date::timestamp + interval '10 hours')
                    AT TIME ZONE 'America/Chicago';

    INSERT INTO calendar.events (
      title, description, start_time,
      notify_before_event, notify_on_the_day, notify_on_the_day_time, notify_at_start
    ) VALUES (
      NEW.doc_title,
      format('Next step: %s%sDue: %s', COALESCE(NEW.next_step, 'unknown'), E'\n', NEW.next_step_date::text),
      v_start_time,
      false, true, '10:00:00'::time, false
    );
  END IF;

  -- Translation workflow deadline
  IF NEW.translation_next_step_date IS DISTINCT FROM OLD.translation_next_step_date
     AND NEW.translation_next_step_date IS NOT NULL THEN

    v_start_time := (NEW.translation_next_step_date::timestamp + interval '10 hours')
                    AT TIME ZONE 'America/Chicago';

    INSERT INTO calendar.events (
      title, description, start_time,
      notify_before_event, notify_on_the_day, notify_on_the_day_time, notify_at_start
    ) VALUES (
      NEW.doc_title || ' (Translation)',
      format('Translation step: %s%sDue: %s', COALESCE(NEW.translation_next_step, 'unknown'), E'\n', NEW.translation_next_step_date::text),
      v_start_time,
      false, true, '10:00:00'::time, false
    );
  END IF;

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- genealogy.trigger_reembed()
--
-- BEFORE UPDATE on documents. When content changes, clears the embedding and
-- embedded_at so the embedding pipeline knows to re-embed, and refreshes
-- content_hash to track the change.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION genealogy.trigger_reembed()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.content IS DISTINCT FROM OLD.content THEN
    NEW.content_hash := md5(NEW.content);
    NEW.embedding    := NULL;
    NEW.embedded_at  := NULL;
  END IF;
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- genealogy.attach_timestamp_triggers()
--
-- DDL event trigger. Auto-attaches created_at / updated_at triggers to any
-- new table created in the genealogy schema.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION genealogy.attach_timestamp_triggers()
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

    IF s_name != 'genealogy' THEN CONTINUE; END IF;

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
          FOR EACH ROW EXECUTE FUNCTION genealogy.set_created_at();', s_name, t_name);
      END IF;
    END IF;

    IF has_updated_at THEN
      SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = format('trg_%s_updated_at', t_name)
        AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass)
        INTO trigger_exists;
      IF NOT trigger_exists THEN
        EXECUTE format('CREATE TRIGGER trg_%I_updated_at BEFORE UPDATE ON %I.%I
          FOR EACH ROW EXECUTE FUNCTION genealogy.set_updated_at();', t_name, s_name, t_name);
      END IF;
    END IF;
  END LOOP;
END;
$$;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- persons
CREATE TRIGGER trg_fill_names_from_links
  BEFORE INSERT OR UPDATE ON genealogy.persons
  FOR EACH ROW EXECUTE FUNCTION genealogy.fill_names_from_links();

CREATE TRIGGER trg_propagate_name_changes
  AFTER UPDATE ON genealogy.persons
  FOR EACH ROW EXECUTE FUNCTION genealogy.propagate_name_changes();

CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON genealogy.persons
  FOR EACH ROW EXECUTE FUNCTION genealogy.set_created_at();

CREATE TRIGGER trg_persons_updated_at
  BEFORE UPDATE ON genealogy.persons
  FOR EACH ROW EXECUTE FUNCTION genealogy.set_updated_at();

-- documents
CREATE TRIGGER trg_update_next_steps
  BEFORE INSERT OR UPDATE ON genealogy.documents
  FOR EACH ROW EXECUTE FUNCTION genealogy.update_next_steps();

CREATE TRIGGER trigger_auto_reembed
  BEFORE UPDATE ON genealogy.documents
  FOR EACH ROW EXECUTE FUNCTION genealogy.trigger_reembed();

CREATE TRIGGER trg_auto_create_document_calendar_events
  AFTER UPDATE ON genealogy.documents
  FOR EACH ROW EXECUTE FUNCTION genealogy.auto_create_document_calendar_events();

CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON genealogy.documents
  FOR EACH ROW EXECUTE FUNCTION genealogy.set_created_at();

CREATE TRIGGER trg_documents_updated_at
  BEFORE UPDATE ON genealogy.documents
  FOR EACH ROW EXECUTE FUNCTION genealogy.set_updated_at();

-- document_person_links
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON genealogy.document_person_links
  FOR EACH ROW EXECUTE FUNCTION genealogy.set_created_at();

CREATE TRIGGER trg_document_person_links_updated_at
  BEFORE UPDATE ON genealogy.document_person_links
  FOR EACH ROW EXECUTE FUNCTION genealogy.set_updated_at();

-- =============================================================================
-- EVENT TRIGGER
-- Auto-attaches timestamp triggers to any new table in the genealogy schema
-- =============================================================================

CREATE EVENT TRIGGER genealogy_timestamp_on_create
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION genealogy.attach_timestamp_triggers();
