import 'dotenv/config';
import fetch from 'node-fetch';
import fs from 'fs/promises';
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_ROLE;

async function execSQL(sql) {
  const res = await fetch(`${SUPA_URL}/sql/v1`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      apikey: SUPA_KEY,
      Authorization: `Bearer ${SUPA_KEY}`,
      Prefer: 'tx=commit'
    },
    body: JSON.stringify({ query: sql })
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`SQL API ${res.status}: ${text}`);
  return text;
}

const path = process.argv[2] || 'schema.sql';
const sql = await fs.readFile(path, 'utf8');
const out = await execSQL(sql);
console.log('Schema OK:', out.slice(0, 200), '...');
