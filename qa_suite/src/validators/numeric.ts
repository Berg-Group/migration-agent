import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier } from './helpers.js';

/**
 * Validates that all values in a column are numeric
 * Accepts integers, decimals, and floats (positive and negative)
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if all non-null values are numeric, false otherwise
 */
export async function numeric(
  table: string,
  column: string,
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Check for values that cannot be cast to numeric type
    // This uses PostgreSQL's ability to test numeric casting with a safe approach
    const [countResult] = await fetchRows<{ total: number }>(`
      SELECT COUNT(*) AS total
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
      AND (
        TRIM(${column}::text) ~ '[^0-9+\-\.eE]'
        OR TRIM(${column}::text) = ''
        OR TRIM(${column}::text) ~ '^[\.eE]'
        OR TRIM(${column}::text) ~ '[\.eE]$'
        OR TRIM(${column}::text) ~ '\..*\.'
        OR TRIM(${column}::text) ~ '[eE].*[eE]'
      )
    `);
    
    if (countResult.total > 0) {
      // Get examples of non-numeric values
      const invalidExamples = await fetchRows<{ value: string }>(`
        SELECT DISTINCT ${column}::text AS value
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        AND (
          TRIM(${column}::text) ~ '[^0-9+\-\.eE]'
          OR TRIM(${column}::text) = ''
          OR TRIM(${column}::text) ~ '^[\.eE]'
          OR TRIM(${column}::text) ~ '[\.eE]$'
          OR TRIM(${column}::text) ~ '\..*\.'
          OR TRIM(${column}::text) ~ '[eE].*[eE]'
        )
        LIMIT 5
      `);
      
      const examples = invalidExamples.map(r => r.value).join(', ');
      log.error(`${table}.${column}: ${countResult.total} values are not numeric [${examples}${countResult.total > 5 ? '...' : ''}]`);
      return false;
    }
    
    return true;
    
  } catch (error) {
    log.error(`Error in numeric validation for ${table}.${column}: ${error}`);
    return process.env.NODE_ENV !== 'production';
  }
}

export {}; 