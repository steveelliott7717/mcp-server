-- =============================================================================
-- Schema: professional_profile
-- Description: Structured CV/resume data plus chunked RAG knowledge base for
--              semantic search over publications, work history, and consulting
--              engagements. Embedding pipeline wired via Supabase Edge Function.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS professional_profile;

-- =============================================================================
-- TABLE: professional_profile.user_profile
-- =============================================================================

CREATE TABLE professional_profile.user_profile (
  id                    bigint      NOT NULL GENERATED ALWAYS AS IDENTITY,
  first_name            text        NOT NULL,
  middle_name           text,
  last_name             text        NOT NULL,
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

  -- Contact
  primary_email         text,
  secondary_email       text,
  phone_number          text,

  -- Address
  address_line1         text,
  address_line2         text,
  city                  text,
  state_province_region text,
  postal_code           text,
  country               text,

  -- Meta
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT user_profile_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: professional_profile.work_experience
-- =============================================================================

CREATE TABLE professional_profile.work_experience (
  id              uuid        NOT NULL DEFAULT gen_random_uuid(),
  title           text        NOT NULL,
  organization    text        NOT NULL,
  department      text,
  location        text,
  employment_type text,
  start_date      date,
  end_date        date,
  summary         text,
  snippet         text,

  -- Structured arrays for granular retrieval
  responsibilities  text[],
  skills            text[],
  tools             text[],
  outcomes          text[],

  url             text,
  display_order   integer     DEFAULT 0,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),

  CONSTRAINT work_experience_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: professional_profile.education
-- =============================================================================

CREATE TABLE professional_profile.education (
  id               uuid        NOT NULL DEFAULT gen_random_uuid(),
  institution_name text        NOT NULL,
  degree_type      text        NOT NULL,
  major            text        NOT NULL,
  subtrack         text,
  start_date       date        NOT NULL,
  end_date         date,
  gpa              numeric,
  location         text,
  description      text,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now(),

  CONSTRAINT education_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: professional_profile.publications
-- =============================================================================

CREATE TABLE professional_profile.publications (
  id               uuid        NOT NULL DEFAULT gen_random_uuid(),
  title            text        NOT NULL,
  citation         text,
  authors          text[],
  publication_type text,
  venue            text,
  year             integer,
  abstract         text,
  snippet          text,
  url              text,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now(),

  CONSTRAINT publications_pkey PRIMARY KEY (id)
);

-- =============================================================================
-- TABLE: professional_profile.publication_chunks
-- Description: Section-level chunks of a publication. Embedding is written
--              back asynchronously by the embed-chunk Edge Function via the
--              update_chunk_embedding() RPC — not stored inline on insert.
-- =============================================================================

CREATE TABLE professional_profile.publication_chunks (
  id              uuid    NOT NULL DEFAULT gen_random_uuid(),
  publication_id  uuid,
  section_number  integer,
  section_title   text,
  page_start      integer,
  page_end        integer,
  content         text,
  created_at      timestamptz DEFAULT now(),

  CONSTRAINT publication_chunks_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_publication_chunks_pub_id  ON professional_profile.publication_chunks USING btree (publication_id);
CREATE INDEX idx_publication_chunks_section ON professional_profile.publication_chunks USING btree (publication_id, section_number);

-- =============================================================================
-- TABLE: professional_profile.work_experience_chunks
-- Description: Chunked representation of work-history documents for RAG.
--              Rich metadata (rag_purpose, domain_keywords, schema_tags) feeds
--              directly into the semantic-search Edge Function's boost scoring.
-- =============================================================================

CREATE TABLE professional_profile.work_experience_chunks (
  id              uuid        NOT NULL DEFAULT gen_random_uuid(),
  experience_id   uuid,
  chunk_index     integer     NOT NULL,
  content         text        NOT NULL,

  -- Embedding (pgvector)
  embedding       vector,
  embedding_model text        DEFAULT 'text-embedding-3-large',

  -- RAG metadata
  chunk_type      text        DEFAULT 'text',   -- 'text' | 'image' | etc.
  rag_purpose     text,                          -- hint for retrieval ranking
  section_title   text,
  section_number  text,
  image_path      text,
  token_count     integer,
  domain_keywords text[],
  schema_tags     text[],

  source_filename text,
  created_at      timestamptz DEFAULT now(),

  CONSTRAINT work_experience_chunks_pkey                  PRIMARY KEY (id),
  CONSTRAINT work_experience_chunks_experience_id_fkey
    FOREIGN KEY (experience_id)
    REFERENCES professional_profile.work_experience (id)
    ON DELETE CASCADE
);

-- =============================================================================
-- TABLE: professional_profile.consulting_chunks
-- Description: Same shape as work_experience_chunks but for consulting
--              engagements. Adds content_hash + embedded_at for idempotent
--              re-embedding: trigger_reembed() clears embedding whenever
--              content changes, and sets content_hash = md5(content) so the
--              embedding pipeline can detect and skip unchanged rows.
-- =============================================================================

CREATE TABLE professional_profile.consulting_chunks (
  id              uuid        NOT NULL DEFAULT gen_random_uuid(),
  chunk_index     integer     NOT NULL,
  content         text        NOT NULL,

  -- Embedding (pgvector)
  embedding       vector,
  embedding_model text        DEFAULT 'text-embedding-3-large',
  embedded_at     timestamptz,
  content_hash    text,       -- md5(content); set by trigger_reembed on change

  -- RAG metadata
  chunk_type      text        DEFAULT 'text',
  rag_purpose     text,
  section_title   text,
  section_number  text,
  domain_keywords text[],
  schema_tags     text[],

  source_filename text,
  token_count     integer,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),

  CONSTRAINT consulting_chunks_pkey        PRIMARY KEY (id),
  CONSTRAINT consulting_chunks_chunk_index_key UNIQUE (chunk_index)
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Timestamp helpers
CREATE OR REPLACE FUNCTION professional_profile.set_created_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.created_at := now();
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION professional_profile.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- professional_profile.trigger_reembed()
--
-- BEFORE UPDATE on consulting_chunks. When content changes:
--   1. Recompute content_hash = md5(content)  — idempotency gate
--   2. Null out embedding and embedded_at      — forces re-embed on next cycle
-- The embedding pipeline (Edge Function) checks embedded_at IS NULL to find
-- rows that need work, so no separate queue table is required.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION professional_profile.trigger_reembed()
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
-- professional_profile.call_embed_chunk()
--
-- SECURITY DEFINER trigger that fires on INSERT or content change for any
-- chunk table. It calls the embed-chunk Supabase Edge Function via
-- net.http_post(), passing {id, content}. The Edge Function calls OpenAI
-- text-embedding-3-large and writes the result back via the
-- update_chunk_embedding() RPC below.
--
-- URL is configured at deploy time via app.supabase_url GUC; replace the
-- hardcoded reference with current_setting('app.supabase_url') if needed.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION professional_profile.call_embed_chunk()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  _url text := current_setting('app.supabase_url') || '/functions/v1/embed-chunk';
BEGIN
  IF (TG_OP = 'INSERT') OR (NEW.content IS DISTINCT FROM OLD.content) THEN
    PERFORM net.http_post(
      url     := _url,
      headers := jsonb_build_object('Content-Type', 'application/json'),
      body    := jsonb_build_object('id', NEW.id, 'content', NEW.content)
    );
  END IF;
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- professional_profile.update_chunk_embedding(chunk_id, new_embedding)
--
-- RPC called by the embed-chunk Edge Function to write the OpenAI embedding
-- back to publication_chunks after async generation.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION professional_profile.update_chunk_embedding(
  chunk_id      uuid,
  new_embedding vector
)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  UPDATE professional_profile.publication_chunks
  SET    embedding = new_embedding
  WHERE  id        = chunk_id;
END;
$$;

-- -----------------------------------------------------------------------------
-- professional_profile.attach_timestamp_triggers()
--
-- DDL event trigger: automatically attaches created_at / updated_at triggers
-- to any new table created in this schema.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION professional_profile.attach_timestamp_triggers()
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

    IF s_name != 'professional_profile' THEN CONTINUE; END IF;

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
        WHERE tgname   = 'trg_force_created_at'
          AND tgrelid  = (quote_ident(s_name) || '.' || quote_ident(t_name))::regclass
      ) INTO trig_exists;

      IF NOT trig_exists THEN
        EXECUTE format(
          'CREATE TRIGGER trg_force_created_at
           BEFORE INSERT ON %I.%I
           FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();',
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
           FOR EACH ROW EXECUTE FUNCTION professional_profile.set_updated_at();',
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

-- user_profile
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.user_profile
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

CREATE TRIGGER trg_user_profile_updated_at
  BEFORE UPDATE ON professional_profile.user_profile
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_updated_at();

-- Cross-schema sync: keeps a parallel copy of user_profile in sync
-- after any INSERT / UPDATE / DELETE.
CREATE TRIGGER trg_sync_user_profile_prof
  AFTER INSERT OR UPDATE OR DELETE ON professional_profile.user_profile
  FOR EACH ROW EXECUTE FUNCTION sync_user_profile_both();

-- work_experience
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.work_experience
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

CREATE TRIGGER trg_work_experience_updated_at
  BEFORE UPDATE ON professional_profile.work_experience
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_updated_at();

-- education
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.education
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

CREATE TRIGGER trg_education_updated_at
  BEFORE UPDATE ON professional_profile.education
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_updated_at();

-- publications
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.publications
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

CREATE TRIGGER trg_publications_updated_at
  BEFORE UPDATE ON professional_profile.publications
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_updated_at();

-- publication_chunks: embedding written back via update_chunk_embedding() RPC
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.publication_chunks
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

-- work_experience_chunks
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.work_experience_chunks
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

-- consulting_chunks: re-embed invalidation + timestamp management
CREATE TRIGGER trg_force_created_at
  BEFORE INSERT ON professional_profile.consulting_chunks
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_created_at();

CREATE TRIGGER trg_consulting_chunks_updated_at
  BEFORE UPDATE ON professional_profile.consulting_chunks
  FOR EACH ROW EXECUTE FUNCTION professional_profile.set_updated_at();

-- Nulls embedding + sets content_hash when content changes, forcing re-embed
CREATE TRIGGER trigger_auto_reembed
  BEFORE UPDATE ON professional_profile.consulting_chunks
  FOR EACH ROW EXECUTE FUNCTION professional_profile.trigger_reembed();

-- =============================================================================
-- EVENT TRIGGER
-- Automatically attaches timestamp triggers to any new table in this schema
-- =============================================================================

CREATE EVENT TRIGGER professional_profile_timestamp_on_create
  ON ddl_command_end
  WHEN TAG IN ('CREATE TABLE')
  EXECUTE FUNCTION professional_profile.attach_timestamp_triggers();
