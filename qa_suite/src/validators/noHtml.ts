import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Checks that a column doesn't contain HTML tags
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if the column doesn't contain HTML tags
 */
export async function noHtml(
  table: string,
  column: string,
): Promise<boolean> {
  // Count rows with HTML content
  const [countResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${column} IS NOT NULL
    AND ${column}::text ~ '<[a-zA-Z][^>]*>'
  `);
  
  if (countResult.total > 0) {
    // Get examples of HTML content
    const htmlExamples = await fetchRows<{ value: string }>(`
      SELECT 
        CASE 
          WHEN LENGTH(${column}::text) > 50 
          THEN LEFT(${column}::text, 47) || '...' 
          ELSE ${column}::text 
        END AS value
      FROM ${table}
      WHERE ${column} IS NOT NULL
      AND ${column}::text ~ '<[a-zA-Z][^>]*>'
      LIMIT 5
    `);
    
    const examples = htmlExamples.map(ex => ex.value).join(', ');
    
    log.error(`${table}.${column}: ${countResult.total} row(s) contain HTML (examples: ${examples})`);
    return false;
  }
  
  return true;
}

export {};
