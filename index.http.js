import 'dotenv/config';
import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import { v5 as uuidv5, v4 as uuidv4 } from 'uuid';
import hash from 'object-hash';
import fs from 'fs/promises';
import path from 'path';
import pg from 'pg';

/** ENV */
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const SCHEMA = process.env.SCHEMA || 'openapi';
const NS = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'; // namespace per UUIDv5

/** PostgreSQL connection via Session Pooler (IPv4 compatible) */
const { Client } = pg;
// Extract project reference from SUPABASE_URL
const projectRef = SUPABASE_URL.replace('https://', '').replace('.supabase.co', '');
// Use session pooler format: aws-0-{region}.pooler.supabase.com
// Note: You may need to adjust the region (e.g., us-east-1, eu-west-1) based on your project
const pgClient = new Client({
  connectionString: process.env.DATABASE_URL || `postgresql://postgres.${projectRef}:${process.env.DB_PASSWORD}@aws-0-us-east-1.pooler.supabase.com:5432/postgres`,
  ssl: { rejectUnauthorized: false }
});

/** Client supabase-js (HTTP) */
const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
  db: { schema: process.env.SCHEMA || 'public' }   // <- qui diciamo allo SDK quale schema usare
});



function walk(obj, cb, path = []) {
  if (obj && typeof obj === 'object') {
    cb(obj, path);
    if (Array.isArray(obj)) {
      obj.forEach((v, i) => walk(v, cb, path.concat([String(i)])));
    } else {
      Object.entries(obj).forEach(([k, v]) => walk(v, cb, path.concat([k])));
    }
  }
}
function getTopKeys(o){ return o && typeof o==='object' ? Object.keys(o) : []; }
const CODE_RE = /^[A-Z]{2,4}\d{2,4}$/; // IIC, IICC, IPL, etc.


/** --------- Utils --------- */
function safeGet(obj, pathArr, def = undefined) {
  return pathArr.reduce((acc, k) => (acc && acc[k] !== undefined ? acc[k] : def), obj);
}
function toSnakeCase(str) {
  return String(str).replace(/([a-z0-9])([A-Z])/g, '$1_$2').replace(/[\s\-.]+/g, '_').toLowerCase().slice(0, 60);
}
function inferPgType(value) {
  if (value === null) return 'text';
  if (typeof value === 'boolean') return 'boolean';
  if (typeof value === 'number') return Number.isInteger(value) ? 'bigint' : 'numeric';
  if (typeof value === 'string') {
    const d = Date.parse(value);
    if (!Number.isNaN(d) && /^(\d{4}-\d{2}-\d{2})/.test(value)) return value.includes('T') ? 'timestamptz' : 'date';
  }
  return 'text';
}
function computeAziendaId(payload) {
  const vat = safeGet(payload, ['companyDetails', 'vatCode']) || 
              safeGet(payload, ['data', 'companyDetails', 'vatCode']) || 
              payload.vatCode || payload?.data?.vatCode;
  const tax = safeGet(payload, ['companyDetails', 'taxCode']) || 
              safeGet(payload, ['data', 'companyDetails', 'taxCode']) || 
              payload.taxCode || payload?.data?.taxCode;
  const base = vat || tax;
  return base ? uuidv5(String(base).trim(), NS) : uuidv4();
}
function effectiveDateForSection(sectionName, sectionPayload, globalFallback) {
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

/** --------- SQL API (DDL dinamico) --------- */
async function execSQL(sql) {
  try {
    if (!pgClient._connected) {
      await pgClient.connect();
    }
    const result = await pgClient.query(sql);
    return result;
  } catch (error) {
    throw new Error(`SQL execution error: ${error.message}`);
  }
}
async function ensureColumn(table, columnName, sampleValue) {
  const col = toSnakeCase(columnName);
  const pgType = inferPgType(sampleValue);
  await execSQL(`
    do $$
    begin
      if not exists (
        select 1 from information_schema.columns
        where table_schema='${SCHEMA}' and table_name='${table}' and column_name='${col}'
      ) then
        execute 'alter table ${SCHEMA}.${table} add column ${col} ${pgType}';
      end if;
    end$$;
  `);
  return { created: true, column: col, pgType };
}

/** --------- DML helper (HTTP) --------- */
async function upsert(table, rows, conflict) {
  const { data, error } = await supa.from(table).upsert(rows, { onConflict: conflict, ignoreDuplicates: false }).select();
  if (error) throw error;
  return data;
}
async function insert(table, rows) {
  const { data, error } = await supa.from(table).insert(rows).select();
  if (error) throw error;
  return data;
}
async function selectOne(table, filters) {
  let q = supa.from(table).select('*').limit(1);
  Object.entries(filters).forEach(([k, v]) => { q = q.eq(k, v); });
  const { data, error } = await q;
  if (error) throw error;
  return data?.[0] || null;
}

/** --------- Sezioni: estrattori --------- */
function extractContacts(payload) {
  const res = { phone: null, email: null, pec: null, website: null };
  walk(payload, (node) => {
    if (!node || typeof node !== 'object' || Array.isArray(node)) return;
    for (const [k, v] of Object.entries(node)) {
      if (typeof v !== 'string') continue;
      const key = k.toLowerCase();
      if (!res.pec    && (key === 'pec' || key.includes('pec') ))  res.pec = v;
      if (!res.email  && (key === 'email' || key === 'e-mail' || key.includes('mail'))) res.email = v;
      if (!res.phone  && (key === 'phone' || key.includes('tel'))) res.phone = v;
      if (!res.website&& (key === 'website' || key.includes('sito') || key.includes('web'))) res.website = v;
    }
  });
  return (res.phone || res.email || res.pec || res.website) ? res : null;
}

function extractAddresses(payload) {
  const results = [];

  // 3A: blocchi noti
  if (payload.address && typeof payload.address === 'object')
    results.push({ addressType: 'SEDE', ...payload.address });
  if (Array.isArray(payload.allOffices)) {
    for (const o of payload.allOffices) {
      const addr = o?.address || o;
      results.push({ addressType: o?.officeType || 'OFFICE', ...addr });
    }
  }

  // 3B: fallback ricorsivo: ogni oggetto che "somiglia" a un indirizzo
  walk(payload, (node, path) => {
    if (!node || typeof node !== 'object' || Array.isArray(node)) return;
    const keys = getTopKeys(node).map(k => k.toLowerCase());
    const hint = ['street','streetname','indirizzo','zip','zipcode','cap','town','city','comune','province','provincia','region','regione','country','stato'];
    const score = hint.reduce((s,h)=> s + (keys.includes(h) ? 1 : 0), 0);
    if (score >= 3) {
      results.push({
        addressType: path.join('.').includes('registered') ? 'SEDE' : (path.join('.').includes('ul') ? 'UL' : 'OFFICE'),
        ...node
      });
    }
  });

  // normalizzazione campi principali
  return results.map(a => ({
    addressType: a.addressType || null,
    street: a.street || a.streetName || a.indirizzo || null,
    zipCode: a.zipCode || a.zip || a.cap || null,
    town: a.town || a.city || a.comune || null,
    province: a.province?.code || a.provincia || a.province || null,
    region: a.region?.description || a.regione || a.region || null,
    country: a.country?.code || a.country || a.stato || null,
    ...a
  }));
}

function extractBalanceEntries(payload) {
  const entries = [];
  
  // Anno di default (può essere estratto da altri campi o impostato manualmente)
  const defaultYear = payload?.year || payload?.fiscalYear || 2024; // aggiornato a 2024
  const defaultCurrency = 'EUR';
  
  // I dati sono dentro payload.data, non direttamente in payload
  const data = payload?.data || payload;
  
  // Mapping allineato con la struttura degli schemi di bilancio
  const sectionMappings = [
    // Stato Patrimoniale - Attivo
    { section: 'stato_patrimoniale_attivo', statement: 'SP_A', description: 'Stato Patrimoniale Attivo' },
    
    // Stato Patrimoniale - Passivo
    { section: 'stato_patrimoniale_passivo', statement: 'SP_P', description: 'Stato Patrimoniale Passivo' },
    
    // Conto Economico
    { section: 'conto_economico', statement: 'CE', description: 'Conto Economico' }
  ];
  
  // Mapping di fallback per il formato euromar (mantenuto per compatibilità)
  const euromarSectionMappings = [
    // Stato Patrimoniale - Attivo
    { section: 'assetsAggregateValues', statement: 'SP_A', description: 'Aggregati Attivo' },
    { section: 'intangibleFixedAssets', statement: 'SP_A', description: 'Immobilizzazioni Immateriali' },
    { section: 'tangibleFixedAssets', statement: 'SP_A', description: 'Immobilizzazioni Materiali' },
    { section: 'cashEquivalents', statement: 'SP_A', description: 'Disponibilità Liquide' },
    { section: 'credits', statement: 'SP_A', description: 'Crediti' },
    
    // Stato Patrimoniale - Passivo
    { section: 'liabilitiesAggregateValues', statement: 'SP_P', description: 'Aggregati Passivo' },
    { section: 'netWorth', statement: 'SP_P', description: 'Patrimonio Netto' },
    { section: 'debts', statement: 'SP_P', description: 'Debiti' },
    
    // Conto Economico
    { section: 'incomeStatementAggregateValues', statement: 'CE', description: 'Aggregati Conto Economico' },
    { section: 'productionValue', statement: 'CE', description: 'Valore della Produzione' },
    { section: 'productionCosts', statement: 'CE', description: 'Costi della Produzione' },
    { section: 'financialIncomeAndCharges', statement: 'CE', description: 'Proventi e Oneri Finanziari' },
    { section: 'extraordinaryIncomeAndCharges', statement: 'CE', description: 'Proventi e Oneri Straordinari' }
  ];
  
  // Le descrizioni dei codici sono ora gestite dalla tabella legend_codes nel database
  
  // Processa prima le sezioni degli schemi di bilancio
  for (const mapping of sectionMappings) {
    const section = data?.[mapping.section];
    if (Array.isArray(section)) {
      for (const item of section) {
        if (item.code && (item.value !== undefined || item.amount !== undefined)) {
          const description = `${mapping.description} - ${item.code}`;
          entries.push({
            year: defaultYear,
            statement: mapping.statement,
            code: item.code,
            description: description,
            amount: item.value ?? item.amount,
            currency: defaultCurrency,
            source_path: mapping.section
          });
        }
      }
    }
  }
  
  // Se non abbiamo trovato dati, prova con il formato euromar
  if (entries.length === 0) {
    for (const mapping of euromarSectionMappings) {
      const section = data?.[mapping.section];
      if (Array.isArray(section)) {
        for (const item of section) {
          if (item.code && (item.value !== undefined || item.amount !== undefined)) {
            const description = `${mapping.description} - ${item.code}`;
            entries.push({
              year: defaultYear,
              statement: mapping.statement,
              code: item.code,
              description: description,
              amount: item.value ?? item.amount,
              currency: defaultCurrency,
              source_path: mapping.section
            });
          }
        }
      }
    }
  }
  
  // Se non abbiamo trovato dati nelle sezioni note, usa il fallback
  if (entries.length === 0) {
    // ultra-fallback: scandisci tutto il JSON e cerca coppie codice/valore
    walk(data, (node, path) => {
      if (!node || typeof node !== 'object') return;
      for (const [k,v] of Object.entries(node)) {
        if (CODE_RE.test(k) && (typeof v === 'number' || typeof v === 'string')) {
          const yearGuess = (''+path.join('.')).match(/20\d{2}/)?.[0]; // estrae un anno se presente nel path
          const description = `Voce di bilancio - ${k}`;
          entries.push({
            year: yearGuess ? Number(yearGuess) : defaultYear,
            statement: k.startsWith('IPL') ? 'SP_P' : (k.startsWith('IIC1') ? 'CE' : 'SP_A'),
            code: k,
            description: description,
            amount: typeof v === 'number' ? v : Number(v),
            currency: defaultCurrency,
            source_path: path.concat([k]).join('.')
          });
        }
      }
    });
  }
  
  return entries.filter(e => e.year && e.code);
}


/** --------- Upsert per sezioni --------- */
async function upsertCompany(payload) {
  const azienda_id = computeAziendaId(payload);
  // Cerca i dati aziendali sia a livello root che dentro data
  const details = payload.companyDetails || payload?.data?.companyDetails || {};
  const data = payload?.data || payload;
  
  const row = {
    azienda_id,
    vat_code: details.vatCode || payload.vatCode || data.vatCode || null,
    tax_code: details.taxCode || payload.taxCode || data.taxCode || null,
    company_name: details.companyName || payload.companyName || data.companyName || null,
    legal_form: payload?.legalForm?.description || data?.legalForm?.description || details.legalForm || null,
    status: payload?.companyStatus?.description || data?.companyStatus?.description || null,
    cciaa: payload?.chamberOfCommerce?.code || data?.chamberOfCommerce?.code || details.cciaa || null,
    rea_code: details.reaCode || data.reaCode || null,
    country_code: payload?.address?.country?.code || data?.address?.country?.code || null
  };
  await upsert('companies', [row], 'azienda_id');
  return azienda_id;
}
async function insertCompanyVersion(azienda_id, raw_json, effective_date) {
  const content_hash = hash(raw_json);
  await insert('company_versions', [{ azienda_id, effective_date, content_hash, raw_json }]);
  return content_hash;
}
async function insertRawSection(azienda_id, section, effective_date, raw_json) {
  await insert('raw_sections', [{ azienda_id, section, effective_date, raw_json }]);
}
async function upsertContacts(azienda_id, sectionPayload, effective_date, report) {
  // promozione colonne scalari extra
  for (const [k, v] of Object.entries(sectionPayload)) {
    if (v !== null && typeof v !== 'object' && !['phone','email','pec','website'].includes(k)) {
      const res = await ensureColumn('contacts', k, v);
      report.createdColumns.push({ table: 'contacts', ...res });
    }
  }
  await insert('contacts', [{
    azienda_id, effective_date,
    phone: sectionPayload.phone, email: sectionPayload.email, pec: sectionPayload.pec, website: sectionPayload.website,
    raw_json: sectionPayload
  }]);
}
async function upsertAddresses(azienda_id, list, report, globalFallback) {
  for (const addr of list) {
    const effective_date = effectiveDateForSection('addresses', addr, globalFallback);
    for (const [k, v] of Object.entries(addr)) {
      if (v !== null && typeof v !== 'object' && !['street','streetName','zipCode','town','province','region','country','addressType'].includes(k)) {
        const res = await ensureColumn('addresses', k, v);
        report.createdColumns.push({ table: 'addresses', ...res });
      }
    }
    await insert('addresses', [{
      azienda_id,
      effective_date,
      address_type: addr.addressType || null,
      street: addr.street || addr.streetName || null,
      zip_code: addr.zipCode || null,
      town: addr.town || null,
      province: addr.province?.code || addr.province || null,
      region: addr.region?.description || addr.region || null,
      country: addr.country?.code || addr.country || null,
      raw_json: addr
    }]);
  }
}
async function upsertBalanceEntries(azienda_id, entries, report) {
  for (const row of entries) {
    if (!row.year || !row.statement || !row.code) {
      report.warnings.push({ table: 'balance_entries', reason: 'missing_keys', row });
      continue;
    }
    // arricchisci descrizione con legend se manca
    let desc = row.description || null;
    if (!desc) {
      const { data, error } = await supa.from(`${SCHEMA}.legend_codes`).select('description').eq('code', row.code).maybeSingle();
      if (!error && data) desc = data.description;
      if (!data) {
        await upsert('unmapped_codes', [{ code: row.code, statement_guess: row.statement }], 'code');
      }
    }
    await upsert('balance_entries', [{
      azienda_id,
      year: row.year,
      statement: row.statement,
      code: row.code,
      description: desc,
      amount: row.amount ?? null,
      currency: row.currency ?? null,
      source_path: row.source_path ?? null,
      note: row.note ?? null,
      content_hash: hash(row)
    }], 'azienda_id,year,statement,code');
  }
}

// Funzione per estrarre dati ATECO
function extractAteco(payload) {
  const ateco = payload?.atecoClassification;
  if (!ateco) return [];
  
  const entries = [];
  
  // ATECO principale
  if (ateco.ateco) {
    entries.push({
      ateco_code: ateco.ateco.code,
      ateco_description: ateco.ateco.description,
      type: 'primary'
    });
  }
  
  // ATECO secondario
  if (ateco.secondaryAteco) {
    entries.push({
      ateco_code: ateco.secondaryAteco,
      ateco_description: null,
      type: 'secondary'
    });
  }
  
  // ATECO 2022
  if (ateco.ateco2022) {
    entries.push({
      ateco_code: ateco.ateco2022.code,
      ateco_description: ateco.ateco2022.description,
      type: 'ateco2022'
    });
  }
  
  // ATECO 2022 secondario
  if (ateco.secondaryAteco2022) {
    entries.push({
      ateco_code: ateco.secondaryAteco2022,
      ateco_description: null,
      type: 'secondary2022'
    });
  }
  
  return entries;
}

// Funzione per inserire dati ATECO
async function upsertAteco(azienda_id, entries, effective_date, report) {
  for (const entry of entries) {
    await insert('ateco', [{
      ateco_id: uuidv4(),
      azienda_id,
      effective_date,
      ateco_code: entry.ateco_code,
      ateco_description: entry.ateco_description,
      raw_json: entry
    }]);
  }
}

/** --------- MAIN --------- */
async function main() {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node index.http.js /path/al/file.json');
    process.exit(1);
  }

  const raw = await fs.readFile(path.resolve(filePath), 'utf8');
  const payload = JSON.parse(raw);
  console.log('Top-level keys:', Object.keys(payload));

  const ingestionId = uuidv4();
  const report = {
    ingestionId,
    createdTables: [],      // (future) se vuoi auto-creare tabelle figlie generiche
    createdColumns: [],
    inserts: {},
    updates: {},
    skips: {},
    warnings: [],
    status: 'PARTIAL'
  };

  try {
    // 1) upsert azienda
    const azienda_id = await upsertCompany(payload);

    // 2) effective_date generale
    const globalEffective = effectiveDateForSection('root', payload, new Date().toISOString());

    // 3) versioning lossless + raw root
    const contentHash = await insertCompanyVersion(azienda_id, payload, globalEffective);
    await insertRawSection(azienda_id, 'root', globalEffective, payload);

    // 4) contacts
    const contacts = extractContacts(payload);
    if (contacts) {
      await upsertContacts(azienda_id, contacts, globalEffective, report);
      report.inserts.contacts = (report.inserts.contacts || 0) + 1;
    } else {
      report.skips.contacts = (report.skips.contacts || 0) + 1;
    }

    // 5) addresses
    const addresses = extractAddresses(payload);
    if (addresses.length) {
      await upsertAddresses(azienda_id, addresses, report, globalEffective);
      report.inserts.addresses = (report.inserts.addresses || 0) + addresses.length;
    } else {
      report.skips.addresses = (report.skips.addresses || 0) + 1;
    }

    // 6) ATECO
    const atecoEntries = extractAteco(payload);
    if (atecoEntries.length) {
      await upsertAteco(azienda_id, atecoEntries, globalEffective, report);
      report.inserts.ateco = (report.inserts.ateco || 0) + atecoEntries.length;
    } else {
      report.skips.ateco = (report.skips.ateco || 0) + 1;
    }

    // 7) bilanci
    const balanceEntries = extractBalanceEntries(payload);
    if (balanceEntries.length) {
      await upsertBalanceEntries(azienda_id, balanceEntries, report);
      report.inserts.balance_entries = (report.inserts.balance_entries || 0) + balanceEntries.length;
    } else {
      report.skips.balance_entries = (report.skips.balance_entries || 0) + 1;
    }

    // 8) log ingestion (HTTP)
    const status = balanceEntries.length + (contacts ? 1 : 0) + addresses.length + atecoEntries.length > 0 ? 'UPDATED' : 'UNCHANGED';
    report.status = status;
    await insert('ingestions', [{
      ingestion_id: ingestionId,
      source: 'it-full',
      started_at: new Date().toISOString(),
      finished_at: new Date().toISOString(),
      azienda_id,
      status,
      summary: report
    }]);

    console.log(JSON.stringify({ ok: true, azienda_id, report }, null, 2));
  } catch (err) {
    await insert('ingestions', [{
      ingestion_id: ingestionId,
      source: 'it-full',
      started_at: new Date().toISOString(),
      finished_at: new Date().toISOString(),
      status: 'ERROR',
      summary: { error: String(err?.message || err) }
    }]).catch(() => {});
    console.error('INGESTION ERROR:', err);
    process.exit(1);
  }
}

main();
