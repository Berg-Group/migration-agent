// Debug script to check what's going on
import { log } from './logger.js';
import { fetchRows } from './db.js';
import { settings } from './config.js';

// Minimal version of the function to list tables
async function listTables(): Promise<string[]> {
  try {
    const rows = await fetchRows<{ fq: string }>(
      `
        SELECT table_schema || '.' || table_name AS fq
          FROM information_schema.tables
         WHERE table_schema = $1
         AND table_name LIKE 'person_identities%'
      `,
      [settings.TARGET_SCHEMA],
    );
    log.info(`Found ${rows.length} tables in schema ${settings.TARGET_SCHEMA}`);
    for (const row of rows) {
      log.info(`Table: ${row.fq}`);
    }
    return rows.map(
      (r) =>
        `"${r.fq.split('.')[0]}"."${r.fq.split('.')[1]}"`, // keep each part quoted
    );
  } catch (error: any) {
    log.error(`Error listing tables: ${error.message}`);
    return [];
  }
}

// Minimal version of the function to list columns in a table
async function listColumns(table: string): Promise<string[]> {
  try {
    const [schema, tbl] = table.replace(/"/g, '').split('.');
    const rows = await fetchRows<{ column_name: string }>(
      `
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name   = $2
      `,
      [schema, tbl],
    );
    log.info(`Found ${rows.length} columns in table ${table}`);
    return rows.map(r => r.column_name);
  } catch (error: any) {
    log.error(`Error listing columns for ${table}: ${error.message}`);
    return [];
  }
}

// Main function
(async () => {
  log.info('Starting debug script...');
  log.info(`Using schema: ${settings.TARGET_SCHEMA}`);
  
  try {
    const tables = await listTables();
    if (tables.length === 0) {
      log.info('No tables found matching the filter');
    }
    
    for (const table of tables) {
      log.info(`Checking columns for table ${table}`);
      const columns = await listColumns(table);
      log.info(`Columns in ${table}:`);
      columns.forEach(col => log.info(`- ${col}`));
      
      log.info('Checking for "atlas_id" column');
      if (columns.includes('atlas_id')) {
        log.info('✅ atlas_id column exists');
      } else {
        log.error('❌ atlas_id column is missing');
      }
    }
    
    log.info('Debug finished successfully');
  } catch (error) {
    log.error(`Debug error: ${error}`);
  }
})(); 