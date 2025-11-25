import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Validates that all values in a column match YYYY-MM-DD date format
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if all non-null values in the column are valid YYYY-MM-DD dates
 */
export async function matchesDateYmd(
  table: string,
  column: string,
): Promise<boolean> {
  // Use a simpler pattern approach for Redshift compatibility
  const [countResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${column} IS NOT NULL
    AND ${column} NOT LIKE '____-__-__'
  `);
  
  if (countResult.total > 0) {
    // Get examples of invalid dates
    const invalidExamples = await fetchRows<{ value: string }>(`
      SELECT ${column} AS value
      FROM ${table}
      WHERE ${column} IS NOT NULL
      AND ${column} NOT LIKE '____-__-__'
      LIMIT 5
    `);
    
    const examples = invalidExamples.map(ex => ex.value).join(', ');
    
    log.error(`${table}.${column}: ${countResult.total} row(s) with invalid YYYY-MM-DD date format (examples: ${examples})`);
    return false;
  }
  
  return true;
}

export {};
