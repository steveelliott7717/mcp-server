-- =============================================================================
-- Schema: gmail
-- Description: Stores all Gmail messages (inbox, sent, drafts) with embeddings
--              for semantic search and a full email open tracking pipeline.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gmail;

-- =============================================================================
-- TABLE: gmail.all_emails
-- =============================================================================

CREATE TABLE gmail.all_emails (
  -- Identity
  id              uuid        NOT NULL DEFAULT gen_random_uuid(),
  message_id      text        NOT NULL,
  thread_id       text        NOT NULL,

  -- Headers
  from_email      text,
  to_email        text,
  cc_email        text,
  bcc_email       text,
  reply_to        text,
  subject         text,
  snippet         text,
  gmail_date      timestamptz,
  message_type    text,           -- CHECK: inbox | sent | draft
  in_reply_to     text,
  reference_ids   text[],

  -- Body
  body_text       text,
  body_html       text,

  -- Flags
  is_reply_to_sent    boolean     DEFAULT false,
  labels              text[],
  is_read             boolean     DEFAULT false,
  is_starred          boolean     DEFAULT false,
  is_important        boolean     DEFAULT false,
  category            text,
  has_attachments     boolean     DEFAULT false,
  attachment_count    integer     DEFAULT 0,
  attachments_archive_url text,
  followup_enabled    boolean     DEFAULT false,

  -- Embeddings (pgvector)
  -- content is auto-populated by the set_email_content trigger (see below)
  content         text,
  embedding       vector,
  embedding_model text            DEFAULT 'text-embedding-3-small',
  embedded_at     timestamptz,

  -- Email open tracking (via track_email_open Edge Function)
  tracked_tag         text,
  is_tracked          boolean     DEFAULT false,
  tracking_active     boolean     DEFAULT true,
  last_checked_at     timestamptz,
  notified_at         timestamptz,
  is_reply_to_tracked boolean     DEFAULT false,
  opened_at           timestamptz,
  open_count          integer     DEFAULT 0,

  -- Meta
  source          text            DEFAULT 'gmail_api',
  raw_payload     jsonb,
  created_at      timestamptz     DEFAULT now(),
  updated_at      timestamptz     DEFAULT now(),

  -- Constraints
  CONSTRAINT all_emails_pkey         PRIMARY KEY (id),
  CONSTRAINT all_emails_message_id_key UNIQUE (message_id),
  CONSTRAINT all_emails_message_type_check
    CHECK (message_type = ANY (ARRAY['inbox'::text, 'sent'::text, 'draft'::text]))
);

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Core lookup indexes
CREATE INDEX idx_all_emails_thread_id    ON gmail.all_emails USING btree (thread_id);
CREATE INDEX idx_all_emails_from_email   ON gmail.all_emails USING btree (from_email);
CREATE INDEX idx_all_emails_gmail_date   ON gmail.all_emails USING btree (gmail_date DESC);
CREATE INDEX idx_all_emails_message_type ON gmail.all_emails USING btree (message_type);

-- GIN index for array containment queries on labels
CREATE INDEX idx_all_emails_labels ON gmail.all_emails USING gin (labels);

-- Partial index: tracking lookup (only rows where tracking is active)
CREATE INDEX idx_all_emails_tracked_tag ON gmail.all_emails
  USING btree (tracked_tag)
  WHERE tracked_tag IS NOT NULL;

CREATE INDEX idx_all_emails_tracking_active ON gmail.all_emails
  USING btree (is_tracked, tracking_active, last_checked_at)
  WHERE is_tracked = true;

-- Partial index: find emails awaiting reply-open notification
CREATE INDEX idx_all_emails_unnotified_replies ON gmail.all_emails
  USING btree (is_reply_to_tracked, notified_at)
  WHERE is_reply_to_tracked = true AND notified_at IS NULL;

-- Unique index on Gmail message_id to prevent duplicates on upsert
CREATE UNIQUE INDEX idx_all_emails_message_id_unique ON gmail.all_emails
  USING btree (message_id);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Timestamp helpers
CREATE OR REPLACE FUNCTION gmail.set_created_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.created_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION gmail.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- gmail.update_email_content()
--
-- BEFORE INSERT OR UPDATE trigger that builds the `content` column used for
-- embedding. Steps:
--   1. Strip HTML tags, scripts and styles from body_html
--   2. Prefer body_text when meaningful; fall back to cleaned HTML
--   3. Remove quoted reply blocks ("On <date> ... wrote:")
--   4. Remove signatures and mobile footers
--   5. Normalize whitespace
--   6. Prepend subject so the embedding captures the topic
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gmail.update_email_content()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  text_body  text;
  html_body  text;
  clean_body text;
BEGIN
  text_body := COALESCE(NEW.body_text, '');
  html_body  := COALESCE(NEW.body_html, '');

  -- Strip <script> and <style> blocks
  html_body := regexp_replace(html_body, E'<(script|style)[^>]*>.*?</\\1>', '', 'gis');
  -- Strip remaining HTML tags
  html_body := regexp_replace(html_body, E'<[^>]+>', ' ', 'g');
  -- Collapse whitespace
  html_body := regexp_replace(html_body, E'\\s+', ' ', 'g');

  -- Use text body when meaningful, otherwise fall back to cleaned HTML
  clean_body := text_body;
  IF length(trim(clean_body)) < 20 THEN
    clean_body := html_body;
  ELSE
    clean_body := clean_body || ' ' || html_body;
  END IF;

  -- Remove quoted reply blocks
  clean_body := regexp_replace(
    clean_body,
    E'On\\s.+wrote:(.|\n)*',
    '',
    'gi'
  );

  -- Remove signatures and mobile footers
  clean_body := regexp_replace(
    clean_body,
    E'(--+\\s*|Sent from my .*|Best,|Thanks,|Regards,|Cheers,)(.|\n)*',
    '',
    'gi'
  );

  -- Final whitespace normalisation
  clean_body := regexp_replace(clean_body, E'\\s+', ' ', 'g');
  clean_body := trim(clean_body);

  NEW.content := trim(
    COALESCE(NEW.subject, '') || E'\n\n' || COALESCE(clean_body, '')
  );

  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- gmail.attach_timestamp_triggers()
--
-- DDL event trigger function. Fires after any CREATE TABLE in the gmail schema
-- and automatically attaches created_at / updated_at triggers so new tables
-- get consistent timestamp management without manual wiring.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gmail.attach_timestamp_triggers()
RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
  obj              RECORD;
  has_created_at   boolean;
  has_updated_at   boolean;
  t_name           text;
  s_name           text;
  trigger_exists   boolean;
BEGIN
  FOR obj IN
    SELECT * FROM pg_event_trigger_ddl_commands()
    WHERE command_tag = 'CREATE TABLE'
  LOOP
    s_name := (
      SELECT n.nspname FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.oid = obj.objid
    );
    t_name := (
      SELECT c.relname FROM pg_class c WHERE c.oid = obj.objid
    );

    IF s_name != 'gmail' THEN CONTINUE; END IF;

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
        WHERE tgname = 'trg_force_created_at'
          AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass
      ) INTO trigger_exists;

      IF NOT trigger_exists THEN
        EXECUTE format(
          'CREATE TRIGGER trg_force_created_at
           BEFORE INSERT ON %I.%I
           FOR EACH ROW EXECUTE FUNCTION gmail.set_created_at();',
          s_name, t_name
        );
      END IF;
    END IF;

    IF has_updated_at THEN
      SELECT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = format('trg_%s_updated_at', t_name)
          AND tgrelid = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass
      ) INTO trigger_exists;

      IF NOT trigger_exists THEN
        EXECUTE format(
          'CREATE TRIGGER trg_%I_updated_at
           BEFORE UPDATE ON %I.%I
           FOR EACH ROW EXECUTE FUNCTION gmail.set_updated_at();',
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

CREATE TRIGGER set_email_content
  BEFORE INSERT OR UPDATE ON gmail.all_emails
  FOR EACH ROW EXECUTE FUNCTION gmail.update_email_content();

CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON gmail.all_emails
  FOR EACH ROW EXECUTE FUNCTION gmail.set_created_at();

CREATE TRIGGER trg_all_emails_updated_at
  BEFORE UPDATE ON gmail.all_emails
  FOR EACH ROW EXECUTE FUNCTION gmail.set_updated_at();

-- =============================================================================
-- EVENT TRIGGER
-- Automatically attaches timestamp triggers to any new table in the gmail schema
-- =============================================================================

CREATE EVENT TRIGGER gmail_timestamp_on_create
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION gmail.attach_timestamp_triggers();
