import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Validates that values in the given column have no leading or trailing whitespace.
 * Returns true if there are zero rows where value != trim(value).
 * NULLs are allowed and ignored.
 */
export async function trim(
  table: string,
  column: string,
): Promise<boolean> {
  // Count rows where value differs from its trimmed version
  const [countResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${column} IS NOT NULL
      AND ${column}::text <> BTRIM(${column}::text)
  `);

  if (countResult.total > 0) {
    const examples = await fetchRows<{ value: string }>(`
      SELECT 
        CASE 
          WHEN LENGTH(${column}::text) > 50 
          THEN LEFT(${column}::text, 47) || '...'
          ELSE ${column}::text
        END AS value
      FROM ${table}
      WHERE ${column} IS NOT NULL
        AND ${column}::text <> BTRIM(${column}::text)
      LIMIT 5
    `);

    const exampleList = examples.map((e) => e.value).join(', ');
    log.error(`${table}.${column}: ${countResult.total} row(s) have leading/trailing spaces (examples: ${exampleList})`);
    return false;
  }

  return true;
}

export {};



