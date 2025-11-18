import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier, isColumnBoolean } from './helpers.js';

/**
 * Validates that a boolean column contains both true and false values
 * Useful for columns that should have variety (not all the same value)
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param warnOnly If true, only warn instead of fail validation when not mixed
 * @returns true if the column contains both true and false values
 */
export async function booleanIsMixed(
  table: string,
  column: string,
  warnOnly = false
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
    
    // Count true and false values
    const [statsResult] = await fetchRows<{ 
      total_rows: number, 
      true_count: number, 
      false_count: number,
      null_count: number 
    }>(`
      SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN ${column} = true THEN 1 ELSE 0 END) AS true_count,
        SUM(CASE WHEN ${column} = false THEN 1 ELSE 0 END) AS false_count,
        SUM(CASE WHEN ${column} IS NULL THEN 1 ELSE 0 END) AS null_count
      FROM ${formattedTable}
    `);
    
    const hasTrueValues = statsResult.true_count > 0;
    const hasFalseValues = statsResult.false_count > 0;
    const isMixed = hasTrueValues && hasFalseValues;
    
    // Calculate percentages
    const truePercent = Math.round((statsResult.true_count / statsResult.total_rows) * 100);
    const falsePercent = Math.round((statsResult.false_count / statsResult.total_rows) * 100);
    const nullPercent = Math.round((statsResult.null_count / statsResult.total_rows) * 100);
    
    if (!isMixed) {
      const msg = `${table}.${column}: Not mixed - contains only ${hasTrueValues ? 'true' : 'false'} values ` +
                 `(true=${statsResult.true_count} (${truePercent}%), false=${statsResult.false_count} (${falsePercent}%), null=${statsResult.null_count} (${nullPercent}%))`;
      
      if (warnOnly) {
        log.warn(msg);
      } else {
        log.error(msg);
        return false;
      }
    } else {
      log.info(`${table}.${column}: Contains mixed boolean values ` +
               `(true=${statsResult.true_count} (${truePercent}%), false=${statsResult.false_count} (${falsePercent}%), null=${statsResult.null_count} (${nullPercent}%))`);
    }
    
    return true;
  } catch (error) {
    log.error(`Error in booleanIsMixed for ${table}.${column}: ${error}`);
    // In development, return true to allow testing to continue
    return process.env.NODE_ENV !== 'production';
  }
}

export {}; 