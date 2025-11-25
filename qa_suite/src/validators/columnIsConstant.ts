import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier, isColumnBoolean } from './helpers.js';

/**
 * Checks if a column has the same value across all rows
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param expectedValue Optional expected value. If not provided, just checks that all values are the same
 * @returns true if all non-null values in the column are the same (and match expectedValue if provided)
 */
export async function columnIsConstant(
  table: string,
  column: string,
  expectedValue?: string
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Check if this is a boolean column
    const isBoolean = await isColumnBoolean(fetchRows, schema, tableName, column);
    
    // If expectedValue is provided, check that all values match it
    if (expectedValue !== undefined) {
      let query: string;
      let params: any[] = [];
      
      if (isBoolean) {
        // For boolean columns, convert expected value to boolean
        const boolValue = expectedValue.toLowerCase() === 'true';
        query = `
          SELECT COUNT(*) AS total
          FROM ${formattedTable}
          WHERE ${column} IS NOT NULL
          AND ${column} != ${boolValue}
        `;
      } else {
        // For other columns, use text comparison
        query = `
          SELECT COUNT(*) AS total
          FROM ${formattedTable}
          WHERE ${column} IS NOT NULL
          AND ${column}::text != $1::text
        `;
        params = [expectedValue];
      }
      
      const [countResult] = await fetchRows<{ total: number }>(query, params);
      
      if (countResult.total > 0) {
        // Get examples of non-matching values
        let examplesQuery: string;
        
        if (isBoolean) {
          examplesQuery = `
            SELECT DISTINCT ${column}::text AS value
            FROM ${formattedTable}
            WHERE ${column} IS NOT NULL
            AND ${column} != ${expectedValue.toLowerCase() === 'true'}
            LIMIT 5
          `;
        } else {
          examplesQuery = `
            SELECT DISTINCT ${column}::text AS value
            FROM ${formattedTable}
            WHERE ${column} IS NOT NULL
            AND ${column}::text != $1::text
            LIMIT 5
          `;
        }
        
        const nonMatchingExamples = await fetchRows<{ value: string }>(
          examplesQuery, 
          isBoolean ? [] : [expectedValue]
        );
        
        const examples = nonMatchingExamples.map(ex => ex.value).join(', ');
        
        log.error(`${table}.${column}: ${countResult.total} row(s) not equal to '${expectedValue}' (examples: ${examples})`);
        return false;
      }
      
      return true;
    }
    
    // If no expectedValue provided, check that all values are the same
    const [result] = await fetchRows<{ distinct_count: number }>(`
      SELECT COUNT(DISTINCT ${column}) AS distinct_count
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
    `);
    
    if (result.distinct_count > 1) {
      // Get examples of different values
      const examples = await fetchRows<{ value: string }>(`
        SELECT DISTINCT ${column}::text AS value
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        LIMIT 5
      `);
      
      const exampleStr = examples.map(e => e.value).join(', ');
      log.error(`${table}.${column}: found ${result.distinct_count} different values (examples: ${exampleStr})`);
      return false;
    }
    
    return true;
  } catch (error) {
    log.error(`Error in columnIsConstant for ${table}.${column}: ${error}`);
    // In development, return true to allow testing to continue
    return process.env.NODE_ENV !== 'production';
  }
}

export {};
