import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier } from './helpers.js';

/**
 * Check if at least a minimum percentage of rows have at least one location field filled
 * 
 * @param table - Fully-qualified table name (e.g., "public.people_202505")
 * @param locationFields - Array of location field names to check
 * @param minCoverage - Minimum coverage threshold (0.0 to 1.0, e.g., 0.5 for 50%)
 * @returns true if coverage meets threshold, false otherwise
 */
export async function locationCoverage(
  table: string,
  locationFields: string[],
  minCoverage: number,
): Promise<boolean> {
  try {
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);

    // Build a WHERE clause that checks if at least one location field is NOT NULL and not empty
    const conditions = locationFields.map(field => 
      `(${field} IS NOT NULL AND TRIM(${field}) != '')`
    ).join(' OR ');

    // Get total row count
    const [totalResult] = await fetchRows<{ total: string }>(`
      SELECT COUNT(*) AS total
      FROM ${formattedTable}
    `);
    const totalRows = parseInt(totalResult.total, 10);

    if (totalRows === 0) {
      log.warn(`${table}: Table is empty, skipping location coverage check`);
      return true;
    }

    // Get count of rows with at least one location field filled
    const [coveredResult] = await fetchRows<{ covered: string }>(`
      SELECT COUNT(*) AS covered
      FROM ${formattedTable}
      WHERE ${conditions}
    `);
    const coveredRows = parseInt(coveredResult.covered, 10);

    // Calculate coverage
    const coverage = coveredRows / totalRows;
    const coveragePercent = (coverage * 100).toFixed(2);
    const thresholdPercent = (minCoverage * 100).toFixed(2);

    log.info(
      `${table}: Location coverage = ${coveragePercent}% ` +
      `(${coveredRows}/${totalRows} rows have at least one location field filled)`
    );

    if (coverage < minCoverage) {
      log.error(
        `${table}: Location coverage ${coveragePercent}% is below threshold ${thresholdPercent}%`
      );
      return false;
    }

    return true;
  } catch (err) {
    log.error(`Error in locationCoverage validator for ${table}: ${err}`);
    return false;
  }
}

export {};

