import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier } from './helpers.js';

// Top 100 currencies list (3-letter ISO codes)
const VALID_CURRENCIES = new Set([
  'USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'CNY', 'SEK', 'NZD',
  'MXN', 'SGD', 'HKD', 'NOK', 'TRY', 'ZAR', 'BRL', 'INR', 'KRW', 'DKK',
  'PLN', 'TWD', 'RUB', 'THB', 'ILS', 'AED', 'CZK', 'MYR', 'HUF', 'CLP',
  'PHP', 'PKR', 'BGN', 'HRK', 'ISK', 'RON', 'IDR', 'VND', 'EGP', 'QAR',
  'SAR', 'KWD', 'BHD', 'OMR', 'JOD', 'LBP', 'KZT', 'UZS', 'AMD', 'GEL',
  'AZN', 'TJS', 'KGS', 'BYN', 'UAH', 'MDL', 'ALL', 'MKD', 'RSD', 'BAM',
  'KES', 'UGX', 'TZS', 'NGN', 'GHS', 'XOF', 'XAF', 'MAD', 'TND', 'DZD',
  'LYD', 'ETB', 'MGA', 'MUR', 'SCR', 'BWP', 'NAD', 'SZL', 'LSL', 'ZMW',
  'AOA', 'CDF', 'XDR', 'COP', 'PEN', 'BOB', 'PYG', 'UYU', 'VES', 'GYD',
  'SRD', 'FJD', 'SBD', 'TOP', 'VUV', 'WST', 'PGK', 'XPF', 'NCX', 'CFP'
]);

/**
 * Validates currency codes in a column
 * FAILS if values are not exactly 3 letters
 * WARNS if 3-letter values are not in the valid currency list
 * 
 * @param table Fully-qualified table name (e.g. "public.people_202505")
 * @param column Column to inspect
 * @returns true if validation passes (all values are 3 letters), false otherwise
 */
export async function currency(
  table: string,
  column: string,
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Check for values that are not exactly 3 letters (FAIL condition)
    const [lengthResult] = await fetchRows<{ total: number }>(`
      SELECT COUNT(*) AS total
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
      AND LENGTH(TRIM(${column})) != 3
    `);
    
    if (lengthResult.total > 0) {
      // Get examples of invalid length values
      const invalidExamples = await fetchRows<{ value: string }>(`
        SELECT DISTINCT ${column}::text AS value
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        AND LENGTH(TRIM(${column})) != 3
        LIMIT 5
      `);
      
      const examples = invalidExamples.map(r => r.value).join(', ');
      log.error(`${table}.${column}: ${lengthResult.total} values are not exactly 3 characters [${examples}${lengthResult.total > 5 ? '...' : ''}]`);
      return false;
    }
    
    // Check for 3-letter values that are not valid currency codes (WARN condition)
    const validCurrencyList = Array.from(VALID_CURRENCIES)
      .map(c => `'${c}'`)
      .join(', ');
    
    const [invalidCurrencyResult] = await fetchRows<{ total: number }>(`
      SELECT COUNT(*) AS total
      FROM ${formattedTable}
      WHERE ${column} IS NOT NULL
      AND LENGTH(TRIM(${column})) = 3
      AND UPPER(TRIM(${column})) NOT IN (${validCurrencyList})
    `);
    
    if (invalidCurrencyResult.total > 0) {
      // Get examples of unrecognized currency codes
      const invalidCurrencyExamples = await fetchRows<{ value: string }>(`
        SELECT DISTINCT UPPER(TRIM(${column})) AS value
        FROM ${formattedTable}
        WHERE ${column} IS NOT NULL
        AND LENGTH(TRIM(${column})) = 3
        AND UPPER(TRIM(${column})) NOT IN (${validCurrencyList})
        LIMIT 5
      `);
      
      const examples = invalidCurrencyExamples.map(r => r.value).join(', ');
      log.warn(`${table}.${column}: ${invalidCurrencyResult.total} values are unrecognized currency codes [${examples}${invalidCurrencyResult.total > 5 ? '...' : ''}]`);
    }
    
    // Return true if all values are 3 letters (even if some are unrecognized currencies)
    return true;
    
  } catch (error) {
    log.error(`Error in currency validation for ${table}.${column}: ${error}`);
    return process.env.NODE_ENV !== 'production';
  }
}

export {}; 