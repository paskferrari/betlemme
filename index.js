import 'dotenv/config';
import { Pool } from 'pg';
import crypto from 'crypto';
import { v5 as uuidv5, v4 as uuidv4 } from 'uuid';
import hash from 'object-hash';
import fs from 'fs/promises';
import path from 'path';

/**
 * Ingestion CLI (singolo JSON):
 *   node index.js /path/al/file.json
 */

const NS = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'; // namespace UUIDv5 (DNS); stabile per generazione azienda_id

const SCHEMA = process.env.SCHEMA || 'openapi';
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});


/* ----------------------------- UTIL --------------------------------- */

function safeGet(obj, pathArr, def = undefined) {
  return pathArr.reduce((acc, key) => (acc && acc[key] !== undefined ? acc[key] : def), obj);
}

// preferenza vatCode -> taxCode
function computeAziendaId(payload) {
  const vat = safeGet(payload, ['companyDetails', 'vatCode']) || payload.vatCode;
  const tax = safeGet(payload, ['companyDetails', 'taxCode']) || payload.taxCode;
  const base = vat || tax;
  if (!base) return uuidv4(); // fallback (sconsigliato ma necessario per lossless)
  return uuidv5(String(base).trim(), NS);
}

function effectiveDateForSection(sectionName, sectionPayload, globalFallback) {
  // Heuristic (puoi raffinare per ciascuna sezione)
  // Bilanci: year -> 31-12-year; altrimenti lastUpdateDate || updateDate || globalFallback
  if (sectionName === 'balance') {
    const year = sectionPayload?.year || sectionPayload?.fiscalYear;
    if (year) return new Date(Date.UTC(Number(year), 11, 31)).toISOString();
  }
  const dates = [
    sectionPayload?.lastUpdateDate,
    sectionPayload?.updateDate,
    sectionPayload?.sinceDate,
    sectionPayload?.roleStartDate
  ].filter(Boolean);
  return (dates[0] ? new Date(dates[0]) : new Date(globalFallback)).toISOString();
}

function isNewer(dateNew, dateOld) {
  if (!dateOld) return true;
  return new Date(dateNew).getTime() > new Date(dateOld).getTime();
}

function toSnakeCase(str) {
  return String(str)
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s\-\.]+/g, '_')
    .toLowerCase()
    .slice(0, 60); // teniamolo corto per il limite 63 char
}

function inferPgType(value) {
  if (value === null) return 'text';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'number') {
    // tenta numeric vs integer
    return Number.isInteger(value) ? 'bigint' : 'numeric';
  }
  // prova date/timestamp iso
  if (typeof value === 'string') {
    const d = Date.parse(value);
    if (!Number.isNaN(d) && /^(\d{4}-\d{2}-\d{2})/.test(value)) {
      // decidiamo date vs timestamptz: se ha 'T' usiamo timestamptz
      return value.includes('T') ? 'timestamptz' : 'date';
    }
  }
  return 'text';
}

/* ---------------------- AUTO-DDL (tabelle/colonne) ------------------- */

async function ensureTable(client, tableName, columns = []) {
  const fq = `${SCHEMA}.${tableName}`;
  await client.query(`create table if not exists ${fq} (like ${SCHEMA}.${tableName} including all);`).catch(async err => {
    if (err.message.includes('does not exist')) {
      // prima creazione: se non esiste uno "schema base", creiamo struttura minima
      await client.query(`create table if not exists ${fq} (
        id uuid primary key default gen_random_uuid(),
        azienda_id uuid references ${SCHEMA}.companies(azienda_id) on delete cascade,
        effective_date timestamptz not null,
        raw_json jsonb,
        created_at timestamptz default now()
      );`);
    } else {
      throw err;
    }
  });

  // promozione colonne scalari proposte
  for (const { name, pgType } of columns) {
    const col = toSnakeCase(name);
    const exists = await client.query(`
      select 1 from information_schema.columns 
      where table_schema=$1 and table_name=$2 and column_name=$3
    `, [SCHEMA, tableName, col]);
    if (exists.rowCount === 0) {
      await client.query(`alter table ${fq} add column ${col} ${pgType};`);
    }
  }
}

async function ensureColumn(client, tableName, columnName, sampleValue) {
  const fq = `${SCHEMA}.${tableName}`;
  const col = toSnakeCase(columnName);
  const exists = await client.query(`
    select 1 from information_schema.columns 
    where table_schema=$1 and table_name=$2 and column_name=$3
  `, [SCHEMA, tableName, col]);

  if (exists.rowCount === 0) {
    const pgType = inferPgType(sampleValue);
    await client.query(`alter table ${fq} add column ${col} ${pgType};`);
    return { created: true, column: col, pgType };
  }
  return { created: false, column: col };
}

/* -------------------------- UPSERTS CORE ------------------------------ */

async function upsertCompany(client, payload) {
  const azienda_id = computeAziendaId(payload);
  const details = payload.companyDetails || {};
  const name = details.companyName || payload.companyName || null;

  const vat = details.vatCode || payload.vatCode || null;
  const tax = details.taxCode || payload.taxCode || null;

  const legal_form = safeGet(payload, ['legalForm', 'description']) || details.legalForm || null;
  const status = safeGet(payload, ['companyStatus', 'description']) || null;

  const cciaa = safeGet(payload, ['chamberOfCommerce', 'code']) || details.cciaa || null;
  const rea = details.reaCode || null;
  const country = safeGet(payload, ['address', 'country', 'code']) || null;

  await client.query(`
    insert into ${SCHEMA}.companies (azienda_id, vat_code, tax_code, company_name, legal_form, status, cciaa, rea_code, country_code)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    on conflict (azienda_id) do update set
      vat_code = excluded.vat_code,
      tax_code = excluded.tax_code,
      company_name = excluded.company_name,
      legal_form = excluded.legal_form,
      status = excluded.status,
      cciaa = excluded.cciaa,
      rea_code = excluded.rea_code,
      country_code = excluded.country_code,
      updated_at = now()
  `, [azienda_id, vat, tax, name, legal_form, status, cciaa, rea, country]);

  return azienda_id;
}

async function insertCompanyVersion(client, azienda_id, raw_json, effective_date) {
  const content_hash = hash(raw_json);
  await client.query(`
    insert into ${SCHEMA}.company_versions (azienda_id, effective_date, content_hash, raw_json)
    values ($1,$2,$3,$4)
    on conflict do nothing
  `, [azienda_id, effective_date, content_hash, raw_json]);
  return content_hash;
}

/* ------------------------ SEZIONI NORMALIZZATE ------------------------ */

async function upsertContacts(client, azienda_id, sectionPayload, effective_date, report) {
  const table = 'contacts';
  const fq = `${SCHEMA}.${table}`;

  // Auto-promuoviamo eventuali nuovi campi scalari frequenti
  for (const [k, v] of Object.entries(sectionPayload)) {
    if (v !== null && typeof v !== 'object') {
      const res = await ensureColumn(client, table, k, v);
      if (res.created) report.createdColumns.push({ table, ...res });
    }
  }

  await client.query(`
    insert into ${fq} (azienda_id, effective_date, phone, email, pec, website, raw_json)
    values ($1,$2,$3,$4,$5,$6,$7)
  `, [azienda_id, effective_date, sectionPayload.phone, sectionPayload.email, sectionPayload.pec, sectionPayload.website, sectionPayload]);
}

async function upsertAddresses(client, azienda_id, list, report, globalFallback) {
  const table = 'addresses';
  const fq = `${SCHEMA}.${table}`;
  for (const addr of list) {
    const effective_date = effectiveDateForSection('addresses', addr, globalFallback);
    // Promozione colonne su campi scalari addizionali
    for (const [k, v] of Object.entries(addr)) {
      if (v !== null && typeof v !== 'object' && !['street','zipCode','town','province','region','country','addressType'].includes(k)) {
        const res = await ensureColumn(client, table, k, v);
        if (res.created) report.createdColumns.push({ table, ...res });
      }
    }
    await client.query(`
      insert into ${fq} (azienda_id, effective_date, address_type, street, zip_code, town, province, region, country, raw_json)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `, [
      azienda_id, effective_date,
      addr.addressType || null,
      addr.street || addr.streetName || null,
      addr.zipCode || null,
      addr.town || null,
      addr.province || addr?.province?.code || null,
      addr.region || addr?.region?.description || null,
      addr.country || addr?.country?.code || null,
      addr
    ]);
  }
}

/* ---------------------- BILANCI & LEGEND (BASIC) --------------------- */

async function upsertBalanceEntries(client, azienda_id, entries, report) {
  const table = 'balance_entries';
  const fq = `${SCHEMA}.${table}`;

  for (const row of entries) {
    // row: {year, statement, code, description?, amount, currency?, source_path, note?}
    if (!row.year || !row.statement || !row.code) {
      report.warnings.push({ table, reason: 'missing_keys', row });
      continue;
    }

    // prova ad arricchire descrizione con legend, se manca
    let desc = row.description || null;
    if (!desc && row.code) {
      const res = await client.query(`select description from ${SCHEMA}.legend_codes where code=$1`, [row.code]);
      if (res.rowCount > 0) desc = res.rows[0].description;
      else {
        // registra unmapped
        await client.query(`
          insert into ${SCHEMA}.unmapped_codes (code, statement_guess, occurrences) 
          values ($1,$2,1)
          on conflict (code) do update set last_seen_at=now(), occurrences=unmapped_codes.occurrences+1
        `, [row.code, row.statement]);
      }
    }

    await client.query(`
      insert into ${fq} (azienda_id, year, statement, code, description, amount, currency, source_path, note, content_hash)
      values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      on conflict (azienda_id, year, statement, code) do update set
        description = excluded.description,
        amount = excluded.amount,
        currency = excluded.currency,
        source_path = excluded.source_path,
        note = excluded.note
    `, [
      azienda_id, row.year, row.statement, row.code, desc,
      row.amount ?? null, row.currency ?? null, row.source_path ?? null, row.note ?? null,
      hash(row)
    ]);
  }
}

/* ---------------------------- RAW / EAV ------------------------------- */

async function insertRawSection(client, azienda_id, section, effective_date, raw_json) {
  await client.query(`
    insert into ${SCHEMA}.raw_sections (azienda_id, section, effective_date, raw_json)
    values ($1,$2,$3,$4)
  `, [azienda_id, section, effective_date, raw_json]);
}

async function upsertUnknownField(client, azienda_id, section, json_path, value) {
  await client.query(`
    insert into ${SCHEMA}.unknown_fields (azienda_id, section, json_path, value_jsonb)
    values ($1,$2,$3,$4)
    on conflict (azienda_id, section, json_path) do update set
      value_jsonb = excluded.value_jsonb,
      last_seen_at = now(),
      occurrences = ${SCHEMA}.unknown_fields.occurrences + 1
  `, [azienda_id, section, json_path, value]);
}

/* --------------------------- ROUTER SEZIONI --------------------------- */

function extractContacts(payload) {
  // PEC/email/telefono possono stare top-level o dentro companyDetails/contacts
  const phone = payload.phone || payload?.contacts?.phone || null;
  const email = payload.email || payload?.contacts?.email || null;
  const pec   = payload.pec || payload?.contacts?.pec || null;
  const website = payload.website || payload?.contacts?.website || null;
  if (!phone && !email && !pec && !website) return null;
  return { phone, email, pec, website };
}

function extractAddresses(payload) {
  const list = [];
  if (payload.address) list.push({ addressType: 'SEDE', ...payload.address });
  if (Array.isArray(payload.allOffices)) {
    for (const o of payload.allOffices) list.push({ addressType: o.officeType || 'OFFICE', ...o.address });
  }
  return list;
}

// Qui inserisci le regole per mappare i blocchi bilancio in entries uniformi
function extractBalanceEntries(payload) {
  const entries = [];
  const year = payload?.balance?.year || payload?.fiscalYear || payload?.year;
  const cur = payload?.balance?.currency || 'EUR';

  // 1) aggregate values -> statement mapping
  const agg = payload?.balance || payload?.financialStatements || {};
  const maps = [
    { path: ['assetsAggregateValues'], statement: 'SP_A' },
    { path: ['liabilitiesAggregateValues'], statement: 'SP_P' },
    { path: ['incomeStatementAggregateValues'], statement: 'CE' },
    // aggiungi altri cluster come productionValue, productionCosts...
  ];

  for (const m of maps) {
    const obj = safeGet(agg, m.path);
    if (obj && typeof obj === 'object') {
      for (const [code, amount] of Object.entries(obj)) {
        entries.push({
          year,
          statement: m.statement,
          code,
          amount: typeof amount === 'number' ? amount : null,
          currency: cur,
          source_path: m.path.join('.')
        });
      }
    }
  }

  // 2) gruppi di dettaglio con coppie {code,value}
  const detailGroups = [
    'intangibleFixedAssets','tangibleFixedAssets','financialFixedAssets',
    'credits','inventory','cashEquivalents','debts','financialAssets',
    'productionValue','productionCosts','revenuesFinancialCharges','annualResult'
  ];
  for (const g of detailGroups) {
    const groupObj = agg?.[g];
    if (groupObj && typeof groupObj === 'object') {
      for (const [code, amount] of Object.entries(groupObj)) {
        // statement guess (semplice, migliorabile)
        const st = ['debts','liabilities','financialCharges','productionCosts'].some(s => g.includes(s)) ? 'SP_P'
                  : ['productionValue','revenues','annualResult'].some(s => g.includes(s)) ? 'CE'
                  : 'SP_A';
        entries.push({
          year,
          statement: st,
          code,
          amount: typeof amount === 'number' ? amount : null,
          currency: cur,
          source_path: `balance.${g}`
        });
      }
    }
  }

  return entries.filter(e => e.year);
}

/* ------------------------------ MAIN --------------------------------- */

async function main() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node index.js /path/al/file.json');
    process.exit(1);
  }

  const raw = await fs.readFile(path.resolve(filePath), 'utf8');
  const payload = JSON.parse(raw);

  const client = await pool.connect();
  const ingestionId = uuidv4();
  const report = {
    ingestionId,
    createdTables: [],
    createdColumns: [],
    inserts: {},
    updates: {},
    skips: {},
    warnings: [],
    status: 'PARTIAL'
  };

  try {
    await client.query('begin');

    // 1) upsert azienda
    const azienda_id = await upsertCompany(client, payload);

    // 2) calcolo effective_date generale
    const globalEffective = effectiveDateForSection('root', payload, new Date().toISOString());

    // 3) versioning lossless
    const contentHash = await insertCompanyVersion(client, azienda_id, payload, globalEffective);

    // 4) RAW root (no-loss)
    await insertRawSection(client, azienda_id, 'root', globalEffective, payload);

    // 5) Contacts
    const contacts = extractContacts(payload);
    if (contacts) {
      await upsertContacts(client, azienda_id, contacts, globalEffective, report);
      report.inserts.contacts = (report.inserts.contacts || 0) + 1;
    } else {
      report.skips.contacts = (report.skips.contacts || 0) + 1;
    }

    // 6) Addresses
    const addresses = extractAddresses(payload);
    if (addresses.length) {
      await upsertAddresses(client, azienda_id, addresses, report, globalEffective);
      report.inserts.addresses = (report.inserts.addresses || 0) + addresses.length;
    } else {
      report.skips.addresses = (report.skips.addresses || 0) + 1;
    }

    // 7) Bilanci
    const balanceEntries = extractBalanceEntries(payload);
    if (balanceEntries.length) {
      await upsertBalanceEntries(client, azienda_id, balanceEntries, report);
      report.inserts.balance_entries = (report.inserts.balance_entries || 0) + balanceEntries.length;
    } else {
      report.skips.balance_entries = (report.skips.balance_entries || 0) + 1;
    }

    // 8) TODO: managers / shareholders / secondary_offices / legal_events / kpi ...
    // In questa versione di scheletro, li tratteremo nella prossima iterazione:
    // - auto-creazione tabelle figlie section_<path> per array complessi
    // - promozione colonne scalari a vista
    // - effective_date per sezione + logica di replace/more-recent

    // 9) write ingestion summary
    const status = balanceEntries.length + (contacts ? 1 : 0) + addresses.length > 0 ? 'UPDATED' : 'UNCHANGED';
    report.status = status;

    await client.query(`
      insert into ${SCHEMA}.ingestions (ingestion_id, source, started_at, finished_at, azienda_id, status, summary)
      values ($1,'it-full',now(),now(),$2,$3,$4)
    `, [ingestionId, azienda_id, status, report]);

    await client.query('commit');

    // Output finale
    console.log(JSON.stringify({ ok: true, azienda_id, report }, null, 2));
  } catch (err) {
    await pool.query(`
      insert into ${SCHEMA}.ingestions (ingestion_id, source, started_at, finished_at, status, summary)
      values ($1,'it-full',now(),now(),'ERROR', jsonb_build_object('error', $2))
    `, [ingestionId, String(err?.message || err)]);
    await pool.query('rollback').catch(() => {});
    console.error('INGESTION ERROR:', err);
    process.exit(1);
  } finally {
    pool.end();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
