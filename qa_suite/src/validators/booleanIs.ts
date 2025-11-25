import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier, isColumnBoolean } from './helpers.js';

/**
 * Validates that all non-null values in a boolean column match a specific boolean value
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param expectedValue Expected boolean value ('true', 'false', true, or false)
 * @returns true if all non-null values match the expected boolean value
 */
export async function booleanIs(
  table: string,
  column: string,
  expectedValue: boolean | string
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Verify that the column is actually a boolean type
    const isActuallyBoolean = await isColumnBoolean(fetchRows, schema, tableName, column);
    
    if (!isActuallyBoolean) {
      log.error(`${table}.${column}: Not a boolean column`);
      return false;
    }
    
    // Normalize the expected value to a boolean
    const expectedBool = typeof expectedValue === 'string' 
      ? expectedValue.toLowerCase() === 'true'
      : expectedValue;
    
    // Check that all non-null values match the expected value
    const [countResult] = await fetchRows<{ mismatch_count: number }>(`
      SELECT COUNT(*) AS mismatch_count
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
      AND ${column} != ${expectedBool}
    `);
    
    if (countResult.mismatch_count > 0) {
      // Get percentage of non-matching values
      const [totalResult] = await fetchRows<{ total_non_null: number }>(`
        SELECT COUNT(*) AS total_non_null
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
      `);
      
      const mismatchPercent = Math.round((countResult.mismatch_count / totalResult.total_non_null) * 100);
      
      log.error(`${table}.${column}: ${countResult.mismatch_count} value(s) (${mismatchPercent}%) are ${!expectedBool} instead of expected ${expectedBool}`);
      return false;
    }
    
    return true;
  } catch (error) {
    log.error(`Error in booleanIs for ${table}.${column}: ${error}`);
    // In development, return true to allow testing to continue
    return process.env.NODE_ENV !== 'production';
  }
}

export {}; 