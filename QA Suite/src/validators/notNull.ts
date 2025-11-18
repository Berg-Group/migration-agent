import { fetchRows } from '../db.js';     // 👈 note ".js" for NodeNext
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier } from './helpers.js';

/**
 * Return `true` when a column contains no NULL values
 * (empty strings are allowed)
 *
 * @param table     Fully-qualified table name  — e.g.  "public.people_202505"
 * @param column    Column to inspect
 * @param warnOnly  Log a WARN (instead of ERROR) but still return `false` on failure
 */
export async function notNull(
  table: string,
  column: string,
  warnOnly = false,
): Promise<boolean> {
  try {
    // Parse table identifier to get schema and table name
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Only check for NULL values
    const [nullResult] = await fetchRows<{ n: string }>(`
      SELECT COUNT(*) AS n
      FROM ${formattedTable}
      WHERE ${column} IS NULL
    `);

    // Important: Convert string count to integer
    const nullCount = parseInt(nullResult.n, 10);
    
    // If we found NULL values, report them
    if (nullCount > 0) {
      const msg = `${table}.${column}: ${nullCount} NULL value(s)`;
      (warnOnly ? log.warn : log.error)(msg);
      return false;
    }
    
    return true;
  } catch (err) {
    log.error(`Error in notNull validator for ${table}.${column}: ${err}`);
    return false;
  }
}

/* An empty export keeps the file in "module" mode even
   if you later comment out or move the named exports.  */
export {};
