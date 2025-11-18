import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Validates that all values in a column are valid email addresses
 * 
 * Simple email validation rules:
 * - Must contain exactly one @ symbol
 * - Must have at least one dot after the @
 * 
 * @param table Fully-qualified table name (e.g. "public.users_202505")
 * @param column Column to inspect (should contain email addresses)
 * @returns true if all non-null values in the column are valid email addresses
 */
export async function email(
  table: string,
  column: string,
): Promise<boolean> {
  
  // SQL query to find invalid emails - only checking for @ and dot after @
  const [countResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${column} IS NOT NULL
    AND (
      -- Must contain exactly one @ symbol
      (LENGTH(${column}) - LENGTH(REPLACE(${column}, '@', ''))) != 1
      -- Must have at least one dot after @
      OR POSITION('.' IN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)) = 0
    )
  `);
  
  if (countResult.total > 0) {
    // Get examples of invalid email values
    const invalidExamples = await fetchRows<{ value: string }>(`
      SELECT ${column} AS value
      FROM ${table}
      WHERE ${column} IS NOT NULL
      AND (
        (LENGTH(${column}) - LENGTH(REPLACE(${column}, '@', ''))) != 1
        OR POSITION('.' IN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)) = 0
      )
      LIMIT 5
    `);
    
    const examples = invalidExamples.map(ex => ex.value).join(', ');
    
    log.error(`${table}.${column}: ${countResult.total} row(s) contain invalid email addresses (examples: ${examples})`);
    return false;
  }
  
  return true;
}

export {}; 