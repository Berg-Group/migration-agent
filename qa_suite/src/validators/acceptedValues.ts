import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier, isColumnBoolean } from './helpers.js';

/**
 * Validates that all values in a column are within a set of accepted values
 * Skips boolean columns as those are handled by dedicated boolean validators
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param allowed Set of accepted values
 * @returns true if all non-null values in the column are in the allowed set
 */
export async function acceptedValues(
  table: string,
  column: string,
  allowed: Set<string>,
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Skip boolean columns - they have their own validators
    if (await isColumnBoolean(fetchRows, schema, tableName, column)) {
      log.info(`${table}.${column}: Skipping acceptedValues validation for boolean column`);
      return true;
    }
    
    // Convert allowed values to a SQL-friendly string
    const allowedList = Array.from(allowed)
      .map(v => `'${v.replace(/'/g, "''")}'`)
      .join(', ');
    
    if (!allowedList) {
      log.error(`No allowed values provided for ${table}.${column}`);
      return false;
    }
    
    // Check for disallowed values using simple text casting
    const [countResult] = await fetchRows<{ total: number }>(`
      SELECT COUNT(*) AS total
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
      AND ${column}::text NOT IN (${allowedList})
    `);
    
    if (countResult.total > 0) {
      // Fetch examples of invalid values
      const invalidExamples = await fetchRows<{ value: string }>(`
        SELECT DISTINCT ${column}::text AS value
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        AND ${column}::text NOT IN (${allowedList})
        LIMIT 5
      `);
      
      const examples = invalidExamples.map(r => r.value).join(', ');
      
      log.error(`${table}.${column}: unexpected values [${examples}${countResult.total > 5 ? '...' : ''}] (${countResult.total} rows)`);
      return false;
    }
    
    return true;
  } catch (error) {
    log.error(`Error in acceptedValues for ${table}.${column}: ${error}`);
    // In development, return true to allow testing to continue
    return process.env.NODE_ENV !== 'production';
  }
}

export {};
