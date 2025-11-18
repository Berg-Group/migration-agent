import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier } from './helpers.js';

/**
 * Validates that a column has unique values
 *
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if all non-null values in the column are unique
 */
export async function unique(
  table: string,
  column: string,
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Find duplicates
    const [result] = await fetchRows<{ dup_count: string }>(`
      SELECT COUNT(*) - COUNT(DISTINCT ${column}) AS dup_count
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
    `);
    
    // Convert string to integer for comparison
    const dupCount = parseInt(result.dup_count, 10);
    
    if (dupCount > 0) {
      // Get examples of duplicates
      const duplicates = await fetchRows<{ 
        value: string; 
        count: string; 
      }>(`
        SELECT ${column}::text AS value, COUNT(*) AS count
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        GROUP BY ${column}
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
      `);
      
      // Format examples for the error message
      const examples = duplicates
        .map(d => `${d.value} (${d.count}×)`)
        .join(', ');
      
      log.error(`${table}.${column}: ${dupCount} duplicate row(s) – e.g. ${examples}`);
      return false;
    }
    
    return true;
  } catch (error) {
    log.error(`Error in unique for ${table}.${column}: ${error}`);
    return false;
  }
}

export {};
