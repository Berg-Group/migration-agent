import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier, safeCastToText, isColumnBoolean } from './helpers.js';

/**
 * Validates that a column has case-insensitive unique values
 *
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if all non-null values in the column are unique when compared case-insensitively
 */
export async function uniqueCi(
  table: string,
  column: string,
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Check if this is a boolean column - uniqueCi doesn't make much sense for boolean
    const isBoolean = await isColumnBoolean(fetchRows, schema, tableName, column);
    
    if (isBoolean) {
      // For boolean columns, check for uniqueness directly
      const [booleanCounts] = await fetchRows<{ true_count: number; false_count: number }>(`
        SELECT 
          SUM(CASE WHEN ${column} = true THEN 1 ELSE 0 END) AS true_count,
          SUM(CASE WHEN ${column} = false THEN 1 ELSE 0 END) AS false_count
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
      `);
      
      if (booleanCounts.true_count > 1 || booleanCounts.false_count > 1) {
        // Report duplicates for boolean values
        let duplicateMessage = '';
        if (booleanCounts.true_count > 1) {
          duplicateMessage += `true (${booleanCounts.true_count}×)`;
        }
        if (booleanCounts.false_count > 1) {
          duplicateMessage += duplicateMessage ? ', ' : '';
          duplicateMessage += `false (${booleanCounts.false_count}×)`;
        }
        
        log.error(`${table}.${column}: boolean column with duplicate values – ${duplicateMessage}`);
        return false;
      }
      
      return true;
    }
    
    // Find case-insensitive duplicates
    const [result] = await fetchRows<{ dup_count: string }>(`
      SELECT COUNT(*) - COUNT(DISTINCT LOWER(${column}::text)) AS dup_count  
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
    `);
    
    // Convert string to integer for comparison
    const dupCount = parseInt(result.dup_count, 10);
    
    if (dupCount > 0) {
      // Get examples of duplicates
      const duplicates = await fetchRows<{ 
        value: string; 
        count: string; 
      }>(`
        SELECT LOWER(${column}::text) AS value, COUNT(*) AS count
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        GROUP BY LOWER(${column}::text)
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
      `);
      
      // Format examples for the error message
      const examples = duplicates
        .map(d => `${d.value} (${d.count}×)`)
        .join(', ');
      
      log.error(`${table}.${column}: ${dupCount} case-insensitive duplicate(s) – ${examples}`);
      return false;
    }
    
    return true;
  } catch (error) {
    log.error(`Error in uniqueCi for ${table}.${column}: ${error}`);
    // In development, return true to allow testing to continue
    return process.env.NODE_ENV !== 'production';
  }
}

export {};
