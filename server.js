import 'dotenv/config';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// PostgreSQL connection
const { Client } = pg;
const pgClient = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Connetti al database
pgClient.connect().catch(err => {
  console.error('Errore connessione database:', err);
  process.exit(1);
});

// Middleware
app.use(express.static(__dirname));
app.use(express.json());

// Endpoint per servire il report HTML
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'report.html'));
});

// API endpoint per recuperare tutti i dati
app.get('/api/report', async (req, res) => {
  try {
    const data = {};
    
    // Recupera dati azienda
    const companyResult = await pgClient.query(`
      SELECT * FROM public.companies 
      ORDER BY created_at DESC 
      LIMIT 1
    `);
    
    if (companyResult.rows.length > 0) {
      data.company = companyResult.rows[0];
      const azienda_id = data.company.azienda_id;
      
      // Recupera contatti
      const contactsResult = await pgClient.query(`
        SELECT * FROM public.contacts 
        WHERE azienda_id = $1 
        ORDER BY created_at DESC
      `, [azienda_id]);
      data.contacts = contactsResult.rows;
      
      // Recupera indirizzi
      const addressesResult = await pgClient.query(`
        SELECT * FROM public.addresses 
        WHERE azienda_id = $1 
        ORDER BY created_at DESC
      `, [azienda_id]);
      data.addresses = addressesResult.rows;
      
      // Recupera dati ATECO
      const atecoResult = await pgClient.query(`
        SELECT * FROM public.ateco 
        WHERE azienda_id = $1 
        ORDER BY created_at DESC
      `, [azienda_id]);
      data.ateco = atecoResult.rows;
      
      // Recupera dati di bilancio
      const balanceResult = await pgClient.query(`
        SELECT * FROM public.balance_entries 
        WHERE azienda_id = $1 
        ORDER BY year DESC, statement, code
      `, [azienda_id]);
      data.balance_entries = balanceResult.rows;
      
      // Recupera versioni azienda
      const versionsResult = await pgClient.query(`
        SELECT version_id, effective_date, content_hash, ingested_at 
        FROM public.company_versions 
        WHERE azienda_id = $1 
        ORDER BY ingested_at DESC
      `, [azienda_id]);
      data.company_versions = versionsResult.rows;
      
      // Recupera sezioni raw
      const rawSectionsResult = await pgClient.query(`
        SELECT raw_id, section, effective_date, created_at 
        FROM public.raw_sections 
        WHERE azienda_id = $1 
        ORDER BY created_at DESC
      `, [azienda_id]);
      data.raw_sections = rawSectionsResult.rows;
      
      // Recupera ingestions
      const ingestionsResult = await pgClient.query(`
        SELECT * FROM public.ingestions 
        WHERE azienda_id = $1 
        ORDER BY started_at DESC
      `, [azienda_id]);
      data.ingestions = ingestionsResult.rows;
    }
    
    // Statistiche generali
    const statsResult = await pgClient.query(`
      SELECT 
        (SELECT COUNT(*) FROM public.companies) as total_companies,
        (SELECT COUNT(*) FROM public.contacts) as total_contacts,
        (SELECT COUNT(*) FROM public.addresses) as total_addresses,
        (SELECT COUNT(*) FROM public.ateco) as total_ateco,
        (SELECT COUNT(*) FROM public.balance_entries) as total_balance_entries,
        (SELECT COUNT(*) FROM public.ingestions) as total_ingestions
    `);
    data.stats = statsResult.rows[0];
    
    res.json(data);
    
  } catch (error) {
    console.error('Errore API:', error);
    res.status(500).json({ 
      error: 'Errore nel recupero dei dati', 
      details: error.message 
    });
  }
});

// Endpoint per recuperare dati di bilancio raggruppati
app.get('/api/balance-summary', async (req, res) => {
  try {
    const result = await pgClient.query(`
      SELECT 
        year,
        statement,
        COUNT(*) as entries_count,
        SUM(amount) as total_amount,
        currency
      FROM public.balance_entries 
      GROUP BY year, statement, currency
      ORDER BY year DESC, statement
    `);
    
    res.json(result.rows);
    
  } catch (error) {
    console.error('Errore API balance summary:', error);
    res.status(500).json({ 
      error: 'Errore nel recupero del riassunto bilanci', 
      details: error.message 
    });
  }
});

// Endpoint per recuperare codici non mappati
app.get('/api/unmapped-codes', async (req, res) => {
  try {
    const result = await pgClient.query(`
      SELECT * FROM public.unmapped_codes 
      ORDER BY occurrences DESC, last_seen_at DESC
    `);
    
    res.json(result.rows);
    
  } catch (error) {
    console.error('Errore API unmapped codes:', error);
    res.status(500).json({ 
      error: 'Errore nel recupero dei codici non mappati', 
      details: error.message 
    });
  }
});

// Gestione errori
app.use((err, req, res, next) => {
  console.error('Errore server:', err);
  res.status(500).json({ 
    error: 'Errore interno del server', 
    details: err.message 
  });
});

// Avvia il server
app.listen(PORT, () => {
  console.log(`🚀 Server avviato su http://localhost:${PORT}`);
  console.log(`📊 Report disponibile su http://localhost:${PORT}`);
});

// Gestione chiusura graceful
process.on('SIGINT', async () => {
  console.log('\n🛑 Chiusura server in corso...');
  await pgClient.end();
  process.exit(0);
});