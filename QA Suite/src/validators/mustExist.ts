import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Checks that required fields exist in the table
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param columns Array of required column names
 * @returns true if all required columns exist
 */
export async function mustExist(
  table: string,
  columns: string[],
): Promise<boolean> {
  if (!columns || columns.length === 0) {
    return true;
  }
  
  // Get all columns in the table
  const [schema, tableName] = table.replace(/"/g, '').split('.');
  const existingColumns = await fetchRows<{ column_name: string }>(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = $1
    AND table_name = $2
  `, [schema, tableName]);
  
  const columnSet = new Set(existingColumns.map(c => c.column_name));
  const missingColumns = columns.filter(c => !columnSet.has(c));
  
  if (missingColumns.length > 0) {
    log.error(`${table}: Missing required column(s): ${missingColumns.join(', ')}`);
    return false;
  }
  
  return true;
}

export {};
