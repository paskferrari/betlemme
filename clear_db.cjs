require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE);

async function clearDatabase() {
  console.log('Svuotamento database in corso...');
  
  const tables = ['balance_entries', 'addresses', 'contacts', 'ateco', 'companies', 'ingestions'];
  
  for (const table of tables) {
    try {
      const { error } = await supa.from(table).delete().neq('id', '00000000-0000-0000-0000-000000000000');
      if (error) {
        console.log(`Errore su ${table}:`, error.message);
      } else {
        console.log(`✅ Svuotata tabella: ${table}`);
      }
    } catch (err) {
      console.log(`❌ Errore su ${table}:`, err.message);
    }
  }
  
  console.log('🎉 Database completamente svuotato!');
}

clearDatabase().catch(console.error);