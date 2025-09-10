-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.addresses (
  address_id uuid NOT NULL DEFAULT gen_random_uuid(),
  azienda_id uuid,
  effective_date timestamp with time zone NOT NULL,
  address_type text,
  street text,
  zip_code text,
  town text,
  province text,
  region text,
  country text,
  raw_json jsonb,
  created_at timestamp with time zone DEFAULT now(),
  tax_code text,
  name text,
  surname text,
  CONSTRAINT addresses_pkey PRIMARY KEY (address_id),
  CONSTRAINT addresses_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.ateco (
  ateco_id uuid NOT NULL,
  azienda_id uuid,
  effective_date timestamp with time zone NOT NULL,
  ateco_code text,
  ateco_description text,
  raw_json jsonb,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT ateco_pkey PRIMARY KEY (ateco_id),
  CONSTRAINT ateco_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.balance_entries (
  entry_id uuid NOT NULL DEFAULT gen_random_uuid(),
  azienda_id uuid,
  year integer NOT NULL,
  statement text NOT NULL CHECK (statement = ANY (ARRAY['SP_A'::text, 'SP_P'::text, 'CE'::text])),
  code text NOT NULL,
  description text,
  amount numeric,
  currency text,
  source_path text,
  note text,
  content_hash text,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT balance_entries_pkey PRIMARY KEY (entry_id),
  CONSTRAINT balance_entries_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.companies (
  azienda_id uuid NOT NULL,
  vat_code text UNIQUE,
  tax_code text UNIQUE,
  company_name text,
  legal_form text,
  status text,
  cciaa text,
  rea_code text,
  country_code text,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT companies_pkey PRIMARY KEY (azienda_id)
);
CREATE TABLE public.company_versions (
  version_id uuid NOT NULL DEFAULT gen_random_uuid(),
  azienda_id uuid,
  effective_date timestamp with time zone NOT NULL,
  content_hash text NOT NULL,
  raw_json jsonb NOT NULL,
  ingested_at timestamp with time zone DEFAULT now(),
  CONSTRAINT company_versions_pkey PRIMARY KEY (version_id),
  CONSTRAINT company_versions_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.contacts (
  contact_id uuid NOT NULL DEFAULT gen_random_uuid(),
  azienda_id uuid,
  effective_date timestamp with time zone NOT NULL,
  phone text,
  email text,
  pec text,
  website text,
  raw_json jsonb,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT contacts_pkey PRIMARY KEY (contact_id),
  CONSTRAINT contacts_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.ingestion_errors (
  error_id uuid NOT NULL,
  ingestion_id uuid,
  azienda_id uuid,
  section text,
  json_path text,
  message text,
  raw_snippet jsonb,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT ingestion_errors_pkey PRIMARY KEY (error_id),
  CONSTRAINT ingestion_errors_ingestion_id_fkey FOREIGN KEY (ingestion_id) REFERENCES public.ingestions(ingestion_id)
);
CREATE TABLE public.ingestions (
  ingestion_id uuid NOT NULL,
  source text DEFAULT 'it-full'::text,
  started_at timestamp with time zone DEFAULT now(),
  finished_at timestamp with time zone,
  azienda_id uuid,
  status text DEFAULT 'PARTIAL'::text CHECK (status = ANY (ARRAY['UPDATED'::text, 'UNCHANGED'::text, 'OUTDATED'::text, 'PARTIAL'::text, 'ERROR'::text])),
  summary jsonb,
  CONSTRAINT ingestions_pkey PRIMARY KEY (ingestion_id)
);
CREATE TABLE public.legend_codes (
  code text NOT NULL,
  description text,
  statement text,
  extra jsonb,
  CONSTRAINT legend_codes_pkey PRIMARY KEY (code)
);
CREATE TABLE public.raw_sections (
  raw_id uuid NOT NULL DEFAULT gen_random_uuid(),
  azienda_id uuid,
  section text NOT NULL,
  effective_date timestamp with time zone NOT NULL,
  raw_json jsonb NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT raw_sections_pkey PRIMARY KEY (raw_id),
  CONSTRAINT raw_sections_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.unknown_fields (
  azienda_id uuid NOT NULL,
  section text NOT NULL,
  json_path text NOT NULL,
  value_jsonb jsonb,
  first_seen_at timestamp with time zone DEFAULT now(),
  last_seen_at timestamp with time zone DEFAULT now(),
  occurrences integer DEFAULT 1,
  CONSTRAINT unknown_fields_pkey PRIMARY KEY (azienda_id, section, json_path),
  CONSTRAINT unknown_fields_azienda_id_fkey FOREIGN KEY (azienda_id) REFERENCES public.companies(azienda_id)
);
CREATE TABLE public.unmapped_codes (
  code text NOT NULL,
  statement_guess text,
  first_seen_at timestamp with time zone DEFAULT now(),
  last_seen_at timestamp with time zone DEFAULT now(),
  occurrences integer DEFAULT 1,
  CONSTRAINT unmapped_codes_pkey PRIMARY KEY (code)
);