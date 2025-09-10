-- Script per svuotare tutte le tabelle del database
-- Ordine di eliminazione rispettando le foreign key constraints

-- Prima eliminiamo le tabelle che dipendono da altre
TRUNCATE TABLE public.ingestion_errors CASCADE;
TRUNCATE TABLE public.addresses CASCADE;
TRUNCATE TABLE public.ateco CASCADE;
TRUNCATE TABLE public.balance_entries CASCADE;
TRUNCATE TABLE public.company_versions CASCADE;
TRUNCATE TABLE public.contacts CASCADE;
TRUNCATE TABLE public.raw_sections CASCADE;
TRUNCATE TABLE public.unknown_fields CASCADE;

-- Poi eliminiamo le tabelle principali
TRUNCATE TABLE public.ingestions CASCADE;
TRUNCATE TABLE public.companies CASCADE;

-- Infine le tabelle indipendenti
TRUNCATE TABLE public.legend_codes CASCADE;
TRUNCATE TABLE public.unmapped_codes CASCADE;

SELECT 'Database svuotato con successo' as status;