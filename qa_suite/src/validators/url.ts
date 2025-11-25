import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Validates that URL-like values in a column do not:
 *  - start with "https://" or "http://" or "www"
 *  - end with a trailing "/"
 *
 * The check is case-insensitive for the starting patterns and trims whitespace
 * before validating.
 *
 * @param table Fully-qualified table name (e.g. "public.company_identities_202505")
 * @param column Column to inspect (string type)
 * @returns true if all non-null values in the column pass the URL rule
 */
export async function url(
  table: string,
  column: string,
): Promise<boolean> {
  // Count rows that violate the rule
  const [countResult] = await fetchRows<{ total: number }>(`
    WITH vals AS (
      SELECT TRIM(${column}) AS v
      FROM ${table}
      WHERE ${column} IS NOT NULL
    )
    SELECT COUNT(*) AS total
    FROM vals
    WHERE 
      v ILIKE 'http://%'
      OR v ILIKE 'https://%'
      OR v ILIKE 'www.%'
      OR RIGHT(v, 1) = '/'
  `);

  if (countResult.total > 0) {
    // Fetch a few example offending values
    const invalidExamples = await fetchRows<{ value: string }>(`
      WITH vals AS (
        SELECT TRIM(${column}) AS v
        FROM ${table}
        WHERE ${column} IS NOT NULL
      )
      SELECT v AS value
      FROM vals
      WHERE 
        v ILIKE 'http://%'
        OR v ILIKE 'https://%'
        OR v ILIKE 'www.%'
        OR RIGHT(v, 1) = '/'
      LIMIT 5
    `);

    const examples = invalidExamples.map(ex => ex.value).join(', ');
    log.error(`${table}.${column}: ${countResult.total} row(s) contain invalid URLs (examples: ${examples})`);
    return false;
  }

  return true;
}

export {};

