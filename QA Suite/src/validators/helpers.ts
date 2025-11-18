import { log } from '../logger.js';

/**
 * Extracts schema and table name from a fully qualified table identifier
 * Handles various formats like "schema"."table" or schema.table
 */
export function parseTableIdentifier(tableId: string): { schema: string; table: string } {
  try {
    // Handle quoted identifiers like "schema"."table"
    if (tableId.includes('"')) {
      const parts = tableId.split('.');
      const schema = parts[0].replace(/"/g, '');
      const table = parts[1].replace(/"/g, '');
      return { schema, table };
    }
    
    // Handle unquoted identifiers like schema.table
    const [schema, table] = tableId.split('.');
    return { schema, table };
  } catch (error) {
    log.error(`Failed to parse table identifier: ${tableId}`);
    // Default fallback - try to handle as-is
    return { 
      schema: tableId.split('.')[0].replace(/"/g, ''), 
      table: tableId.split('.')[1]?.replace(/"/g, '') || 'unknown_table' 
    };
  }
}

/**
 * Creates a properly formatted table identifier for use in SQL queries
 * Redshift is sometimes picky about quoting, this helps ensure compatibility
 */
export function formatTableIdentifier(schema: string, table: string): string {
  // Clean up any existing quotes
  const cleanSchema = schema.replace(/"/g, '');
  const cleanTable = table.replace(/"/g, '');
  
  // Format appropriately for Redshift
  return `"${cleanSchema}"."${cleanTable}"`;
}

/**
 * A safe cast for all column types that works in Redshift
 * Avoids the "cannot cast type boolean to character varying" error
 * Also avoids the "operator does not exist: character varying = boolean" error
 */
export function safeCastToText(column: string): string {
  // Simple basic cast to text - we'll handle type-specific validation in separate boolean validators
  return `COALESCE(${column}::text, '')`;
}

/**
 * Determines if a column is of boolean type
 */
export async function isColumnBoolean(
  fetchRows: Function, 
  schema: string, 
  tableName: string, 
  columnName: string
): Promise<boolean> {
  try {
    const [columnInfo] = await fetchRows(`
      SELECT data_type 
      FROM information_schema.columns 
      WHERE table_schema = $1 
      AND table_name = $2 
      AND column_name = $3
    `, [schema, tableName, columnName]);
    
    return columnInfo && columnInfo.data_type === 'boolean';
  } catch (err) {
    log.warn(`Could not determine column type for ${schema}.${tableName}.${columnName}`);
    return false;
  }
}

/**
 * Comparison helper that handles boolean and string values
 * For use when comparing column values to expected values
 */
export function safeEquals(column: string, value: string): string {
  // For boolean values
  if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
    const boolValue = value.toLowerCase() === 'true';
    return `(${column} = ${boolValue})`;
  }
  
  // For other values, use text comparison
  return `(${column}::text = '${value.replace(/'/g, "''")}'::text)`;
}

export {}; 