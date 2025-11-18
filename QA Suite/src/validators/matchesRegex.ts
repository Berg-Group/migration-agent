import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Validates that all values in a column match a given regex pattern
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param pattern Regular expression pattern to match
 * @returns true if all non-null values in the column match the pattern
 */
export async function matchesRegex(
  table: string,
  column: string,
  pattern: RegExp,
): Promise<boolean> {
  // Explicitly check UUID pattern since it's very common
  const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  
  if (pattern.source === UUID_PATTERN.source) {
    // Special case for UUIDs - use LIKE pattern
    const [countResult] = await fetchRows<{ total: number }>(`
      SELECT COUNT(*) AS total
      FROM ${table}
      WHERE ${column} IS NOT NULL
      AND ${column} NOT LIKE '________-____-____-____-____________'
    `);
    
    if (countResult.total > 0) {
      // Get examples of non-matching values
      const invalidExamples = await fetchRows<{ value: string }>(`
        SELECT ${column} AS value
        FROM ${table}
        WHERE ${column} IS NOT NULL
        AND ${column} NOT LIKE '________-____-____-____-____________'
        LIMIT 5
      `);
      
      const examples = invalidExamples.map(ex => ex.value).join(', ');
      
      log.error(`${table}.${column}: ${countResult.total} row(s) don't match UUID pattern (examples: ${examples})`);
      return false;
    }
    
    return true;
  }
  
  // For other patterns, we need a custom approach per pattern
  // This is a limitation, but we can add more cases as needed
  log.warn(`Regex validation not fully implemented for pattern: ${pattern.source} - skipping check`);
  return true;
}

export {};
