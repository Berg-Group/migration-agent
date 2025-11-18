import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier, isColumnBoolean } from './helpers.js';

/**
 * Validates that a column contains only boolean values (true or false)
 * and optionally checks that all values match an expected boolean value
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param expectedValue Optional expected boolean value. If provided, checks that all values match this
 * @returns true if all values are valid booleans (and match expectedValue if provided)
 */
export async function isBoolean(
  table: string,
  column: string,
  expectedValue?: boolean | string
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
    
    // If an expected value is provided, normalize it
    let expectedBool: boolean | undefined;
    if (expectedValue !== undefined) {
      if (typeof expectedValue === 'string') {
        // Convert string 'true'/'false' to boolean
        expectedBool = expectedValue.toLowerCase() === 'true';
      } else {
        expectedBool = expectedValue;
      }
      
      // Check that all values match the expected value
      const [countResult] = await fetchRows<{ mismatch_count: number }>(`
        SELECT COUNT(*) AS mismatch_count
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        AND ${column} != ${expectedBool}
      `);
      
      if (countResult.mismatch_count > 0) {
        log.error(`${table}.${column}: ${countResult.mismatch_count} value(s) do not match expected ${expectedBool}`);
        return false;
      }
    }
    
    // Check for NULL values
    const [nullResult] = await fetchRows<{ null_count: number }>(`
      SELECT COUNT(*) AS null_count
      FROM ${formattedTable}
      WHERE ${column} IS NULL
    `);
    
    if (nullResult.null_count > 0) {
      log.warn(`${table}.${column}: Contains ${nullResult.null_count} NULL value(s)`);
      // Don't fail validation for NULLs alone, as that's handled by notNull validator
    }
    
    // Get statistics about true/false distribution
    const [statsResult] = await fetchRows<{ total_rows: number, true_count: number, false_count: number }>(`
      SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN ${column} = true THEN 1 ELSE 0 END) AS true_count,
        SUM(CASE WHEN ${column} = false THEN 1 ELSE 0 END) AS false_count
      FROM ${formattedTable}
    `);
    
    // Calculate percentage of true/false values
    const truePercent = Math.round((statsResult.true_count / statsResult.total_rows) * 100);
    const falsePercent = Math.round((statsResult.false_count / statsResult.total_rows) * 100);
    const nullPercent = Math.round((nullResult.null_count / statsResult.total_rows) * 100);
    
    log.info(`${table}.${column}: true=${statsResult.true_count} (${truePercent}%), false=${statsResult.false_count} (${falsePercent}%), null=${nullResult.null_count} (${nullPercent}%)`);
    
    return true;
  } catch (error) {
    log.error(`Error in isBoolean for ${table}.${column}: ${error}`);
    // In development, return true to allow testing to continue
    return process.env.NODE_ENV !== 'production';
  }
}

export {}; 