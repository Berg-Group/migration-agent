// src/db.ts
import { Pool, QueryResultRow } from 'pg';
import { settings } from './config.js';
import { log } from './logger.js';

// Create pool with connection info from settings
const pool = new Pool({
  host: settings.REDSHIFT_HOST,
  port: settings.REDSHIFT_PORT,
  database: settings.REDSHIFT_DB,
  user: settings.REDSHIFT_USER,
  password: settings.REDSHIFT_PASSWORD,
  // pg handles keep-alive automatically
});

// Log connection information (without password)
log.info(`Database connection configured for: ${settings.REDSHIFT_USER}@${settings.REDSHIFT_HOST}:${settings.REDSHIFT_PORT}/${settings.REDSHIFT_DB}`);
log.info(`Target schema: ${settings.TARGET_SCHEMA}`);

/**
 * Run a parametrised query and return the rows.
 * Usage: const rows = await fetchRows<{ id:number }>('SELECT id FROM t WHERE x=$1', [42])
 */
export async function fetchRows<T extends QueryResultRow = QueryResultRow>(
  sql: string,
  params: unknown[] = [],
): Promise<T[]> {
  try {
    const client = await pool.connect();
    try {
      // Log the SQL query for debugging (with truncation for long queries)
      const truncatedSql = sql.length > 200 ? `${sql.substring(0, 200)}...` : sql;
      log.debug(`Executing SQL: ${truncatedSql}`);
      if (params.length > 0) {
        log.debug(`With parameters: ${JSON.stringify(params)}`);
      }
      
      const res = await client.query(sql, params);
      return res.rows as T[];
    } finally {
      client.release();
    }
  } catch (error: unknown) {
    const err = error as Error;
    
    // Provide detailed error info
    log.error(`Database error: ${err.message}`);
    log.error(`Query causing error: ${sql.substring(0, 300)}${sql.length > 300 ? '...' : ''}`);
    if (params.length > 0) {
      log.error(`Query parameters: ${JSON.stringify(params)}`);
    }
    
    // No fallback to mock data - throw the error
    throw err;
  }
}

/** Convenience for single-value result (`SELECT COUNT(*) AS value …`) */
export async function fetchScalar<T>(sql: string, params: unknown[] = []): Promise<T> {
  const [row] = await fetchRows<{ value: T }>(sql, params);
  if (!row) {
    throw new Error(`No rows returned for scalar query: ${sql}`);
  }
  return row.value;
}
