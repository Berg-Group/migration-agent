import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * List of disallowed email domains
 * Includes fake, test, disposable, and temporary email domains
 */
const DISALLOWED_DOMAINS = new Set([
  // Fake/placeholder domains
  'noemail.com',
  'fakeemail.com',
  'fake.com',
  'invalid.com',
  'dummy.com',
  'placeholder.com',
  
  // Test/example domains
  'test.com',
  'testing.com',
  'example.com',
  'example.org',
  'example.net',
  'sample.com',
  'demo.com',
  
  // Disposable/temporary email services
  'mailinator.com',
  'temp-mail.com',
  'tempmail.com',
  'guerrillamail.com',
  '10minutemail.com',
  'throwaway.email',
  'trashmail.com',
  'maildrop.cc',
  'yopmail.com',
  'sharklasers.com',
  'grr.la',
  'spam4.me',
  'tempr.email',
  'emailondeck.com',
  'getnada.com',
  'anonbox.net',
  'burnermail.io',
  'getairmail.com',
]);

/**
 * Comprehensive email validation
 * 
 * This validator enforces strict email validation rules including:
 * 
 * Structure:
 * - Exactly one @ symbol
 * - No spaces anywhere
 * - At least one dot after @
 * - No leading/trailing dots or hyphens
 * 
 * Local Part (before @):
 * - Cannot start or end with a dot
 * - No consecutive dots
 * - Valid characters only: A-Z a-z 0-9 . _ % + -
 * 
 * Domain Part (after @):
 * - Must contain at least one dot
 * - No consecutive dots
 * - Domain segments cannot start/end with hyphens
 * - TLD (last segment) must be at least 2 characters, letters only
 * 
 * Data Quality:
 * - No leading/trailing whitespace
 * - No non-ASCII characters (unless explicitly supporting IDN)
 * - Blocks known fake/disposable/test domains (see DISALLOWED_DOMAINS)
 * 
 * @param table Fully-qualified table name
 * @param column Column to inspect (should contain email addresses)
 * @returns true if all non-null values are valid emails
 */
export async function validEmail(
  table: string,
  column: string
): Promise<boolean> {
  
  // Build the SQL validation query
  const validationConditions = `
    ${column} IS NOT NULL
    AND (
      -- Must contain exactly one @ symbol
      (LENGTH(${column}) - LENGTH(REPLACE(${column}, '@', ''))) != 1
      
      -- No spaces anywhere
      OR ${column} LIKE '% %'
      
      -- No leading or trailing whitespace
      OR ${column} != TRIM(${column})
      
      -- Must have at least one dot after @
      OR POSITION('.' IN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)) = 0
      
      -- Email cannot start or end with a dot
      OR ${column} LIKE '.%'
      OR ${column} LIKE '%.'
      
      -- Email cannot start or end with a hyphen
      OR ${column} LIKE '-%'
      OR ${column} LIKE '%-'
      
      -- Local part (before @) cannot start with a dot
      OR SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1) LIKE '.%'
      
      -- Local part (before @) cannot end with a dot
      OR SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1) LIKE '%.'
      
      -- Local part cannot contain consecutive dots (..)
      OR SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1) LIKE '%..%'
      
      -- Domain part (after @) cannot contain consecutive dots
      OR SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) LIKE '%..%'
      
      -- Domain part must not start or end with a dot
      OR SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) LIKE '.%'
      OR SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) LIKE '%.'
      
      -- Local part: check for invalid characters (anything not A-Z a-z 0-9 . _ % + -)
      -- Using REGEXP_INSTR to find any character that's not in the allowed set
      OR REGEXP_INSTR(
           SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1),
           '[^A-Za-z0-9._\\%\\+\\-]'
         ) > 0
      
      -- Domain part: check for invalid characters (must be A-Z a-z 0-9 . -)
      OR REGEXP_INSTR(
           SUBSTRING(${column}, POSITION('@' IN ${column}) + 1),
           '[^A-Za-z0-9.\\-]'
         ) > 0
      
      -- Domain segment cannot start with hyphen (check for @- or .-)
      OR SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) ~ '(^-|\\.-)' 
      
      -- Domain segment cannot end with hyphen (check for -. or -$ at end)
      OR SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) ~ '(-\\.|-$)'
      
      -- TLD (last segment after last dot) must be at least 2 characters
      OR LENGTH(
           SUBSTRING(
             SUBSTRING(${column}, POSITION('@' IN ${column}) + 1),
             POSITION('.' IN REVERSE(SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)))
           )
         ) < 2
      
      -- TLD must contain only letters (no numbers or special chars)
      OR REGEXP_INSTR(
           REVERSE(
             SUBSTRING(
               REVERSE(SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)),
               1,
               POSITION('.' IN REVERSE(SUBSTRING(${column}, POSITION('@' IN ${column}) + 1))) - 1
             )
           ),
           '[^A-Za-z]'
         ) > 0
      
      -- Check for non-ASCII characters (emoji, unicode, etc.)
      OR REGEXP_INSTR(${column}, '[^[:ascii:]]') > 0
    )
  `;

  // Count invalid emails
  const [countResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${validationConditions}
  `);
  
  let hasErrors = false;
  
  if (countResult.total > 0) {
    hasErrors = true;
    
    // Get examples of invalid emails
    const invalidExamples = await fetchRows<{ value: string; reason: string }>(`
      SELECT 
        ${column} AS value,
        CASE
          WHEN (LENGTH(${column}) - LENGTH(REPLACE(${column}, '@', ''))) != 1 THEN 'wrong number of @ symbols'
          WHEN ${column} LIKE '% %' THEN 'contains spaces'
          WHEN ${column} != TRIM(${column}) THEN 'leading/trailing whitespace'
          WHEN POSITION('.' IN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)) = 0 THEN 'no dot in domain'
          WHEN ${column} LIKE '.%' OR ${column} LIKE '%.' THEN 'starts or ends with dot'
          WHEN ${column} LIKE '-%' OR ${column} LIKE '%-' THEN 'starts or ends with hyphen'
          WHEN SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1) LIKE '.%' 
            OR SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1) LIKE '%.' THEN 'local part starts/ends with dot'
          WHEN SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1) LIKE '%..%' THEN 'local part has consecutive dots'
          WHEN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) LIKE '%..%' THEN 'domain has consecutive dots'
          WHEN REGEXP_INSTR(SUBSTRING(${column}, 1, POSITION('@' IN ${column}) - 1), '[^A-Za-z0-9._\\%\\+\\-]') > 0 
            THEN 'invalid character in local part'
          WHEN REGEXP_INSTR(SUBSTRING(${column}, POSITION('@' IN ${column}) + 1), '[^A-Za-z0-9.\\-]') > 0 
            THEN 'invalid character in domain'
          WHEN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) ~ '(^-|\\.-)' THEN 'domain segment starts with hyphen'
          WHEN SUBSTRING(${column}, POSITION('@' IN ${column}) + 1) ~ '(-\\.|-$)' THEN 'domain segment ends with hyphen'
          WHEN REGEXP_INSTR(${column}, '[^[:ascii:]]') > 0 THEN 'contains non-ASCII characters'
          ELSE 'invalid TLD'
        END AS reason
      FROM ${table}
      WHERE ${validationConditions}
      LIMIT 10
    `);
    
    log.error(`${table}.${column}: ${countResult.total} row(s) contain invalid email addresses`);
    invalidExamples.forEach(ex => {
      log.error(`  ❌ "${ex.value}" - ${ex.reason}`);
    });
  }
  
  // Check for disallowed domains
  const domainList = Array.from(DISALLOWED_DOMAINS).map(d => `'${d.toLowerCase()}'`).join(',');
  
  const [disallowedResult] = await fetchRows<{ total: number }>(`
    SELECT COUNT(*) AS total
    FROM ${table}
    WHERE ${column} IS NOT NULL
      AND (LENGTH(${column}) - LENGTH(REPLACE(${column}, '@', ''))) = 1
      AND LOWER(
            SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)
          ) IN (${domainList})
  `);
  
  if (disallowedResult.total > 0) {
    hasErrors = true;
    
    // Get examples
    const disallowedExamples = await fetchRows<{ value: string; domain: string }>(`
      SELECT 
        ${column} AS value,
        LOWER(SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)) AS domain
      FROM ${table}
      WHERE ${column} IS NOT NULL
        AND (LENGTH(${column}) - LENGTH(REPLACE(${column}, '@', ''))) = 1
        AND LOWER(
              SUBSTRING(${column}, POSITION('@' IN ${column}) + 1)
            ) IN (${domainList})
      LIMIT 10
    `);
    
    log.error(`${table}.${column}: ${disallowedResult.total} row(s) use disallowed domains`);
    disallowedExamples.forEach(ex => {
      log.error(`  ❌ "${ex.value}" - blocked domain: ${ex.domain}`);
    });
  }
  
  return !hasErrors;
}

export {}; 

