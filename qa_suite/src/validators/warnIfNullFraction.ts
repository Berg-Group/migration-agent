import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Warns if the fraction of NULL values in a column exceeds a threshold
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @param threshold Warning threshold (0-1), e.g. 0.5 for 50%
 * @returns true if the NULL fraction is below threshold, false otherwise
 */
export async function warnIfNullFraction(
  table: string,
  column: string,
  threshold: number
): Promise<boolean> {
  if (threshold < 0 || threshold > 1) {
    log.warn(`Invalid threshold ${threshold} for ${table}.${column} NULL check (must be 0-1)`);
    return false;
  }
  
  // Calculate NULL fraction
  const [result] = await fetchRows<{ null_count: number; total_count: number; fraction: number }>(`
    SELECT 
      SUM(CASE WHEN ${column} IS NULL THEN 1 ELSE 0 END)::float AS null_count,
      COUNT(*)::float AS total_count,
      SUM(CASE WHEN ${column} IS NULL THEN 1 ELSE 0 END)::float / COUNT(*)::float AS fraction
    FROM ${table}
    WHERE 1=1
  `);
  
  // Convert to percentage for readability
  const nullPercentage = (result.fraction * 100).toFixed(1);
  
  if (result.fraction > threshold) {
    log.warn(`${table}.${column}: ${nullPercentage}% NULLs (>${threshold * 100}% threshold)`);
    return false;
  }
  
  return true;
}

export {};
