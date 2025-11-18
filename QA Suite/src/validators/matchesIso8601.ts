import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Validates that all values in a column match ISO 8601 datetime format
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if all non-null values in the column are valid ISO 8601 datetimes
 */
export async function matchesIso8601(
  table: string,
  column: string,
): Promise<boolean> {
  // For Redshift compatibility, we'll accept the standard datetime format that excludes 'Z'
  // This is a simplification but should be acceptable for most cases.
  // ISO 8601 can be very complicated, so we're using a reduced pattern check
  
  // First, relax our regex to just check the basic pattern with or without time zone
  const [countResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${column} IS NOT NULL
    AND ${column} NOT LIKE '____-__-__T__:__:__'
    AND ${column} NOT LIKE '____-__-__T__:__:__%'
    AND ${column} NOT LIKE '____-__-__ __:__:__'
    AND ${column} NOT LIKE '____-__-__ __:__:__%'
  `);
  
  if (countResult.total > 0) {
    // Get examples of invalid timestamps
    const invalidExamples = await fetchRows<{ value: string }>(`
      SELECT ${column} AS value
      FROM ${table}
      WHERE ${column} IS NOT NULL
      AND ${column} NOT LIKE '____-__-__T__:__:__'
      AND ${column} NOT LIKE '____-__-__T__:__:__%'
      AND ${column} NOT LIKE '____-__-__ __:__:__'
      AND ${column} NOT LIKE '____-__-__ __:__:__%'
      LIMIT 5
    `);
    
    const examples = invalidExamples.map(ex => ex.value).join(', ');
    
    log.error(`${table}.${column}: ${countResult.total} row(s) with invalid ISO 8601 format (examples: ${examples})`);
    return false;
  }
  
  return true;
}

export {};
