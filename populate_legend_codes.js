import 'dotenv/config';
import fs from 'fs';
import { createClient } from '@supabase/supabase-js';

// Configurazione Supabase
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const SCHEMA = process.env.SCHEMA || 'public';

const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false },
  db: { schema: SCHEMA }
});

async function populateLegendCodes() {
  try {
    console.log('Connesso a Supabase');

    // Leggi il file degli schemi
    const schemaData = JSON.parse(fs.readFileSync('./schemi_bilancio_conto_completo.json', 'utf8'));
    
    // Svuota la tabella legend_codes
    const { error: deleteError } = await supa.from('legend_codes').delete().neq('code', '');
    if (deleteError) {
      console.error('Errore durante lo svuotamento:', deleteError);
      return;
    }
    console.log('Tabella legend_codes svuotata');

    let insertCount = 0;
    const batchSize = 100;
    const allCodes = [];

    // Prepara tutti i codici per l'inserimento batch
    for (const item of schemaData.stato_patrimoniale_attivo) {
      allCodes.push({
        code: item.code,
        description: item.description,
        statement: 'SP_A'
      });
    }

    for (const item of schemaData.stato_patrimoniale_passivo) {
      allCodes.push({
        code: item.code,
        description: item.description,
        statement: 'SP_P'
      });
    }

    for (const item of schemaData.conto_economico) {
      allCodes.push({
        code: item.code,
        description: item.description,
        statement: 'CE'
      });
    }

    // Inserisci in batch
    for (let i = 0; i < allCodes.length; i += batchSize) {
      const batch = allCodes.slice(i, i + batchSize);
      const { error } = await supa.from('legend_codes').insert(batch);
      if (error) {
        console.error(`Errore durante l'inserimento batch ${i}-${i + batch.length}:`, error);
      } else {
        insertCount += batch.length;
        console.log(`Inseriti ${batch.length} codici (totale: ${insertCount})`);
      }
    }

    console.log(`\nInseriti ${insertCount} codici nella tabella legend_codes`);
    
    // Verifica alcuni inserimenti
    const { data: countData, error: countError } = await supa
      .from('legend_codes')
      .select('*', { count: 'exact', head: true });
    
    if (!countError) {
      console.log(`Totale codici in tabella: ${countData?.length || 'N/A'}`);
    }
    
    // Mostra alcuni esempi
    const { data: examples, error: exampleError } = await supa
      .from('legend_codes')
      .select('code, description, statement')
      .order('statement')
      .order('code')
      .limit(10);
    
    if (!exampleError && examples) {
      console.log('\nEsempi di codici inseriti:');
      examples.forEach(row => {
        console.log(`${row.code} (${row.statement}): ${row.description}`);
      });
    }

  } catch (error) {
    console.error('Errore durante il popolamento:', error);
  }
}

// Esegui lo script
populateLegendCodes();