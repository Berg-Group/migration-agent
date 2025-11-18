// ─────────── src/tests/tableTests.ts ───────────
import fs from 'node:fs';
import path from 'node:path';
import yaml from 'js-yaml';
import { minimatch } from 'minimatch';
import { log } from '../logger.js';
import { settings } from '../config.js';
import {
  fetchRows,
  fetchScalar,
} from '../db.js';
import {
  notNull,
  unique,
  uniqueCi,
  acceptedValues,
  matchesRegex,
  matchesIso8601,
  matchesDateYmd,
  warnIfNullFraction,
  errorIfNullFraction,
  columnIsConstant,
  mustExist,
  noHtml,
  UUID_RE,
  // Import boolean validators
  isBoolean,
  booleanIs,
  booleanIsMixed,
  // Import position validators
  consecutiveAttributePositions,
  // Import candidate duplicate validator
  candidateDuplicates,
  // Import email validator
  email,
  // Import currency validator
  currency,
  // Import numeric validator
  numeric,
  // Import URL validator
  url,
  // Import trim validator
  trim as trimValidator,
  // Import location coverage validator
  locationCoverage,
} from '../validators/index.js';

/* ------------------------------------------------------------------ */
/*                          Load Rules                                 */
/* ------------------------------------------------------------------ */
let RULES: any = {
  global: {
    iso_timestamp: { columns: ['created_at', 'updated_at'] },
    not_null: ['atlas_id'],
    required: ['created_at', 'updated_at']
  },
  tables: {}
};

try {
  // Load rules.yml file
  const rulesPath = path.resolve(path.dirname(new URL(import.meta.url).pathname), 'rules.yml');
  log.info(`Loading rules from ${rulesPath}`);
  const yamlContent = fs.readFileSync(rulesPath, 'utf8');
  const loadedRules = yaml.load(yamlContent) as any;
  
  // Verify the structure and use the loaded rules
  if (loadedRules && typeof loadedRules === 'object' && loadedRules.global && loadedRules.tables) {
    RULES = loadedRules;
    log.info(`Successfully loaded rules.yml with ${Object.keys(RULES.tables).length} table patterns`);
    
    // Log global rules for debugging
    if (RULES.global.required) {
      log.info(`Global required columns: ${JSON.stringify(RULES.global.required)}`);
    }
    if (RULES.global.not_null) {
      log.info(`Global not_null columns: ${JSON.stringify(RULES.global.not_null)}`);
    }
    if (RULES.global.iso_timestamp && RULES.global.iso_timestamp.columns) {
      log.info(`Global timestamp columns: ${JSON.stringify(RULES.global.iso_timestamp.columns)}`);
    }
  } else {
    const errorMsg = `There is an error in ${rulesPath}, we cannot start the QA. Rules file has invalid structure (missing 'global' or 'tables' sections).`;
    log.error(errorMsg);
    console.error(`\n❌ ${errorMsg}\n`);
    process.exit(1);
  }
} catch (error: any) {
  const rulesPath = path.resolve(path.dirname(new URL(import.meta.url).pathname), 'rules.yml');
  const errorMsg = `There is an error in ${rulesPath}, we cannot start the QA. ${error.message}`;
  log.error(errorMsg);
  console.error(`\n❌ ${errorMsg}\n`);
  process.exit(1);
}

/* ------------------------------------------------------------------ */
/*                       Database Access                               */
/* ------------------------------------------------------------------ */
function prefixClause(): string {
  // Base clause - if no prefixes are specified, include all tables
  let baseClause = 'TRUE';
  
  // Include clause - only include tables with specified prefixes, if any
  if (settings.TABLE_PREFIXES.length) {
    baseClause = settings.TABLE_PREFIXES
      .map((p) => `table_name ~ '^${p.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}'`)
      .join(' OR ');
  }
  
  // Exclude clause - exclude tables with specified prefixes
  if (settings.EXCLUDED_TABLE_PREFIXES.length) {
    // Build an exclusion clause for each prefix to exclude
    const excludeClause = settings.EXCLUDED_TABLE_PREFIXES
      .map((p) => `table_name !~ '^${p.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}'`)
      .join(' AND ');
    
    // Combine base clause with exclusion
    return `(${baseClause}) AND (${excludeClause})`;
  }
  
  return baseClause;
}

async function listTables(): Promise<string[]> {
  try {
    log.info(`Using schema: ${settings.TARGET_SCHEMA}`);
    log.info(`Table prefix filter: ${prefixClause()}`);
    const rows = await fetchRows<{ fq: string }>(
      `
        SELECT table_schema || '.' || table_name AS fq
          FROM information_schema.tables
         WHERE table_schema = $1
           AND (${prefixClause()})
      `,
      [settings.TARGET_SCHEMA],
    );
    log.info(`Found ${rows.length} tables in schema ${settings.TARGET_SCHEMA}`);
    if (rows.length > 0) {
      log.info(`Tables found:`);
      for (const row of rows) {
        log.info(`  - ${row.fq}`);
      }
    } else {
      log.warn(`No tables found in schema ${settings.TARGET_SCHEMA}!`);
    }
    return rows.map(
      (r) =>
        `"${r.fq.split('.')[0]}"."${r.fq.split('.')[1]}"`, // keep each part quoted
    );
  } catch (error: any) {
    log.error(`Error listing tables: ${error.message}`);
    return [];
  }
}

async function listColumns(table: string): Promise<Set<string>> {
  try {
    const [schema, tbl] = table.replace(/"/g, '').split('.');
    const rows = await fetchRows<{ column_name: string }>(
      `
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = $1
           AND table_name   = $2
      `,
      [schema, tbl],
    );
    const columnSet = new Set(rows.map((r) => r.column_name));
    log.info(`Found ${rows.length} columns in table ${table}`);
    // Log all columns for debugging
    const columnList = Array.from(columnSet).join(', ');
    log.info(`Columns: ${columnList}`);
    return columnSet;
  } catch (error: any) {
    log.error(`Error listing columns for ${table}: ${error.message}`);
    return new Set();
  }
}

/* ------------------------------------------------------------------ */
/*                     Core Validation Functions                       */
/* ------------------------------------------------------------------ */
async function applyGlobalRules(
  table: string,
  cols: Set<string>,
  fails: string[],
): Promise<void> {
  // Extract raw table name for pattern matching
  const rawName = table.split('.')[1].replace(/"/g, '');
  
  // Check global required columns
  if (RULES.global.required) {
    for (const col of RULES.global.required) {
      // Skip atlas_id check for project_company_contacts tables
      if (col === 'atlas_id' && tableShouldMatchPattern(rawName, 'project_company_contacts*')) {
        log.info(`Skipping required global column '${col}' check for ${table} (exception)`);
        continue;
      }
      
      if (!cols.has(col)) {
        log.error(`${table}: Missing required global column '${col}'`);
        fails.push(`${col}: missing`);
      }
    }
  }
  
  // Check global not_null columns
  if (RULES.global.not_null) {
    for (const col of RULES.global.not_null) {
      // Skip atlas_id check for project_company_contacts tables
      if (col === 'atlas_id' && tableShouldMatchPattern(rawName, 'project_company_contacts*')) {
        log.info(`Skipping required global not_null column '${col}' check for ${table} (exception)`);
        continue;
      }
      
      if (!cols.has(col)) {
        log.error(`${table}: Missing required global not_null column '${col}'`);
        fails.push(`${col}: missing`);
      } else if (!(await notNull(table, col))) {
        log.error(`${table}: Column '${col}' contains NULL values`);
        fails.push(`${col}: NULLs`);
      }
    }
  }
  
  // Check global iso_timestamp columns
  if (RULES.global.iso_timestamp && RULES.global.iso_timestamp.columns) {
    for (const col of RULES.global.iso_timestamp.columns) {
      if (cols.has(col)) {
        if (!(await matchesIso8601(table, col))) {
          log.error(`${table}: Column '${col}' contains invalid ISO timestamps`);
          fails.push(`${col}: bad ISO format`);
        }
      }
    }
  }
}

// Helper for field validation
const validateField = async (
  table: string,
  column: string,
  cols: Set<string>,
  validator: (t: string, c: string) => Promise<boolean>,
  errorMsg: string,
  fails: string[],
): Promise<void> => {
  if (cols.has(column)) {
    try {
      if (!(await validator(table, column))) {
        log.error(`${table}: Column '${column}' ${errorMsg}`);
        fails.push(`${column}: ${errorMsg}`);
      }
    } catch (error) {
      log.warn(`Error validating ${table}.${column}: ${error}`);
    }
  }
};

// Helper for validating each item in an array
const validateEach = async (
  table: string,
  columns: string[] | undefined,
  cols: Set<string>,
  validator: (t: string, c: string) => Promise<boolean>,
  errorMsg: string,
  fails: string[],
): Promise<void> => {
  if (!columns) return;
  for (const col of columns) {
    await validateField(table, col, cols, validator, errorMsg, fails);
  }
};

async function applyTableRules(
  pattern: string,
  rule: any,
  table: string,
  cols: Set<string>,
  fails: string[],
): Promise<void> {
  log.info(`Applying rules for pattern '${pattern}' to table ${table}`);
  
  try {
    // Required columns check
    if (rule.required) {
      let requiredCols: string[] = [];
      if (Array.isArray(rule.required)) {
        requiredCols = rule.required;
      } else if (typeof rule.required === 'object') {
        // Convert object notation to array if needed
        requiredCols = Object.keys(rule.required);
      }
      
      for (const col of requiredCols) {
        if (!cols.has(col)) {
          log.error(`${table}: Missing required column '${col}'`);
          fails.push(`${col}: missing`);
        }
      }
    }
    
    // Nice-to-have columns (just warnings)
    if (rule.nice_to_have) {
      let niceCols: string[] = [];
      if (Array.isArray(rule.nice_to_have)) {
        niceCols = rule.nice_to_have;
      } else if (typeof rule.nice_to_have === 'object') {
        niceCols = Object.keys(rule.nice_to_have);
      }
      
      for (const col of niceCols) {
        if (!cols.has(col)) {
          log.warn(`${table}: Nice-to-have column '${col}' is missing`);
        }
      }
    }
    
    // not_null columns check
    if (rule.not_null) {
      for (const col of rule.not_null) {
        if (cols.has(col)) {
          try {
            if (!(await notNull(table, col))) {
              log.error(`${table}: Column '${col}' contains NULL values`);
              fails.push(`${col}: NULLs`);
            }
          } catch (error) {
            log.warn(`Error checking null values for ${table}.${col}: ${error}`);
          }
        }
      }
    }
    
    // UUID columns check
    if (rule.uuid_columns) {
      await validateEach(
        table, 
        rule.uuid_columns, 
        cols, 
        (t, c) => matchesRegex(t, c, UUID_RE), 
        'contains invalid UUIDs', 
        fails
      );
    }
    
    // Uniqueness checks
    if (rule.unique) {
      await validateEach(
        table,
        rule.unique,
        cols,
        unique,
        'contains duplicate values',
        fails
      );
    }
    
    if (rule.unique_ci) {
      await validateEach(
        table,
        rule.unique_ci,
        cols,
        uniqueCi,
        'contains case-insensitive duplicates',
        fails
      );
    }
    
    // Date format checks
    if (rule.date_ymd_columns) {
      await validateEach(
        table,
        rule.date_ymd_columns,
        cols,
        matchesDateYmd,
        'contains invalid YYYY-MM-DD dates',
        fails
      );
    }
    
    // ISO timestamp checks for table-specific
    if (rule.iso_timestamp) {
      await validateEach(
        table,
        rule.iso_timestamp,
        cols,
        matchesIso8601,
        'contains invalid ISO timestamps',
        fails
      );
    }
    
    // HTML exclusion checks
    if (rule.no_html_columns) {
      await validateEach(
        table,
        rule.no_html_columns,
        cols,
        noHtml,
        'contains HTML',
        fails
      );
    }
    
    // Email validation checks
    if (rule.email) {
      await validateEach(
        table,
        rule.email,
        cols,
        email,
        'contains invalid email addresses',
        fails
      );
    }
    
    // Currency validation checks
    if (rule.currency) {
      await validateEach(
        table,
        rule.currency,
        cols,
        currency,
        'contains invalid currency codes',
        fails
      );
    }
    
    // Numeric validation checks
    if (rule.numeric) {
      await validateEach(
        table,
        rule.numeric,
        cols,
        numeric,
        'contains non-numeric values',
        fails
      );
    }

    // URL validation checks
    if (rule.url) {
      await validateEach(
        table,
        rule.url,
        cols,
        url,
        'contains invalid URLs',
        fails
      );
    }
    
    // Trim checks (no leading/trailing whitespace)
    if (rule.trim) {
      await validateEach(
        table,
        rule.trim,
        cols,
        trimValidator,
        'has leading/trailing whitespace',
        fails
      );
    }
    
    // Location coverage check
    if (rule.location_coverage) {
      try {
        const minCoverage = Number(rule.location_coverage);
        const locationFields = ['location_street_address', 'location_locality', 'location_postal_code', 'location_country'];
        
        // Only check fields that exist in the table
        const existingLocationFields = locationFields.filter(field => cols.has(field));
        
        if (existingLocationFields.length > 0) {
          if (!(await locationCoverage(table, existingLocationFields, minCoverage))) {
            const threshold = (minCoverage * 100).toFixed(0);
            log.error(`${table}: Location coverage is below ${threshold}% threshold`);
            fails.push(`location_coverage: below ${threshold}% threshold`);
          }
        } else {
          log.warn(`${table}: location_coverage rule specified but no location fields found`);
        }
      } catch (error) {
        log.warn(`Error checking location coverage for ${table}: ${error}`);
      }
    }
    
    // Enum value checks
    if (rule.allowed_values) {
      for (const [col, values] of Object.entries(rule.allowed_values)) {
        if (cols.has(col)) {
          try {
            if (!(await acceptedValues(table, col, new Set(values as string[])))) {
              log.error(`${table}: Column '${col}' contains disallowed values`);
              fails.push(`${col}: invalid values`);
            }
          } catch (error) {
            log.warn(`Error checking allowed values for ${table}.${col}: ${error}`);
          }
        }
      }
    }

    // Boolean validation checks
    // -------------------------

    // Check for columns that should be boolean
    if (rule.isBoolean) {
      const booleanCols = Array.isArray(rule.isBoolean) 
        ? rule.isBoolean 
        : Object.keys(rule.isBoolean);
      
      await validateEach(
        table,
        booleanCols,
        cols,
        isBoolean,
        'is not a valid boolean column',
        fails
      );
    }

    // Check for columns that should be specific boolean values
    if (rule.boolean_is) {
      for (const [col, expectedValue] of Object.entries(rule.boolean_is)) {
        if (cols.has(col)) {
          try {
            const typedValue = (typeof expectedValue === 'string' && (expectedValue === 'true' || expectedValue === 'false'))
              ? expectedValue === 'true' 
              : Boolean(expectedValue);
            
            if (!(await booleanIs(table, col, typedValue))) {
              log.error(`${table}: Column '${col}' is not all ${typedValue}`);
              fails.push(`${col}: not all ${typedValue}`);
            }
          } catch (error) {
            log.warn(`Error checking boolean values for ${table}.${col}: ${error}`);
          }
        }
      }
    }

    // Check for columns that should have both true and false values
    if (rule.booleanIsMixed) {
      for (const [col, warnOnly] of Object.entries(rule.booleanIsMixed)) {
        if (cols.has(col)) {
          try {
            if (!(await booleanIsMixed(table, col, warnOnly === true))) {
              if (warnOnly !== true) {
                log.error(`${table}: Column '${col}' does not have a mix of true and false`);
                fails.push(`${col}: not mixed boolean`);
              }
            }
          } catch (error) {
            log.warn(`Error checking boolean mix for ${table}.${col}: ${error}`);
          }
        }
      }
    }
    
    // NULL fraction warnings
    if (rule.warn_null) {
      for (const [col, threshold] of Object.entries(rule.warn_null)) {
        if (cols.has(col)) {
          try {
            await warnIfNullFraction(table, col, Number(threshold));
          } catch (error) {
            log.warn(`Error checking NULL fraction for ${table}.${col}: ${error}`);
          }
        }
      }
    }
    
    // NULL fraction errors (hard thresholds)
    if (rule.error_null) {
      for (const [col, threshold] of Object.entries(rule.error_null)) {
        if (cols.has(col)) {
          try {
            const numThreshold = Number(threshold);
            if (!(await errorIfNullFraction(table, col, numThreshold))) {
              log.error(`${table}: Column '${col}' has too many NULL values (>${numThreshold * 100}% threshold)`);
              fails.push(`${col}: excessive NULLs (>${numThreshold * 100}%)`);
            }
          } catch (error) {
            log.warn(`Error checking NULL fraction for ${table}.${col}: ${error}`);
          }
        }
      }
    }
    
    // Constant column checks
    if (rule.constant) {
      for (const [col, expectedValue] of Object.entries(rule.constant)) {
        if (cols.has(col)) {
          try {
            if (!(await columnIsConstant(table, col, String(expectedValue)))) {
              log.error(`${table}: Column '${col}' is not constant with value '${expectedValue}'`);
              fails.push(`${col}: not '${expectedValue}'`);
            }
          } catch (error) {
            log.warn(`Error checking constant value for ${table}.${col}: ${error}`);
          }
        }
      }
    }
    
    // Constant across table checks
    if (rule.constant_across_table) {
      for (const col of rule.constant_across_table) {
        if (cols.has(col)) {
          try {
            const val = await fetchScalar<string>(`SELECT MIN(${col}) AS value FROM ${table}`);
            if (!(await columnIsConstant(table, col, val))) {
              log.error(`${table}: Column '${col}' doesn't have the same value across all rows`);
              fails.push(`${col}: mixed values`);
            }
          } catch (error) {
            log.warn(`Error checking constant across table for ${table}.${col}: ${error}`);
          }
        }
      }
    }
    
    // Check consecutive positions for attribute options
    if (rule.consecutive_attribute_positions) {
      let attributeIdColumn = 'atlas_attribute_id';
      let positionColumn = 'position';
      
      // Handle configuration options if provided as an object
      if (typeof rule.consecutive_attribute_positions === 'object') {
        const config = rule.consecutive_attribute_positions;
        attributeIdColumn = config.attribute_id_column || attributeIdColumn;
        positionColumn = config.position_column || positionColumn;
      }
      
      try {
        if (!(await consecutiveAttributePositions(table, attributeIdColumn, positionColumn))) {
          log.error(`${table}: Positions for attributes are not consecutive from 1 to n`);
          fails.push(`${attributeIdColumn}/${positionColumn}: non-consecutive positions`);
        }
      } catch (error) {
        log.warn(`Error checking consecutive positions for ${table}: ${error}`);
      }
    }
  } catch (error) {
    log.error(`Error applying rules for pattern '${pattern}' to table ${table}: ${error}`);
  }
}

/**
 * Counts the number of rows in a table
 */
async function countRows(table: string): Promise<number> {
  try {
    const result = await fetchRows<{ count: string }>(`SELECT COUNT(*) as count FROM ${table}`);
    return parseInt(result[0].count, 10);
  } catch (error: any) {
    log.error(`Error counting rows for ${table}: ${error.message}`);
    return -1;
  }
}

/* ------------------------------------------------------------------ */
/*                       Main QA Function                              */
/* ------------------------------------------------------------------ */
export async function runColumnTests(): Promise<Record<string, string[]>> {
  const failures: Record<string, string[]> = {};

  const noRuleTables: string[] = [];
  const usedPatterns: Set<string> = new Set();
  const totalTables = (await listTables()).length;
  let processedCount = 0;

  for (const table of await listTables()) {
    processedCount++;
    
    // Count the rows in the table
    const rowCount = await countRows(table);
    const rowCountText = rowCount >= 0 ? `${rowCount} rows` : 'unknown row count';
    
    // Always log when we start and complete a table, even in quiet mode
    console.log(`\n[${processedCount}/${totalTables}] 🔍 STARTING validation for ${table} (${rowCountText})`);
    log.info(`🔍  Checking ${table} (${rowCountText})`);
    
    const cols = await listColumns(table);
    const tblFails: string[] = [];

    // 1. Check global rules first
    await applyGlobalRules(table, cols, tblFails);

    // 2. Find matching pattern for this table
    const rawName = table.split('.')[1].replace(/"/g, '');
    log.info(`Table raw name: ${rawName}`);
    
    // Print all patterns to verify order
    log.info(`Checking patterns: ${Object.keys(RULES.tables).join(', ')}`);
    
    // Look for the first matching pattern in the original file order
    // Since YAML preserves order from the file, we can check in order
    let matchedPattern = null;
    for (const pattern of Object.keys(RULES.tables)) {
      // Use our helper for expanded pattern matching
      const shouldMatch = tableShouldMatchPattern(rawName, pattern);
      
      if (shouldMatch) {
        log.info(`Table ${rawName} matches pattern ${pattern}`);
        usedPatterns.add(pattern);
        matchedPattern = pattern;
        
        // Apply pattern-specific rules
        await applyTableRules(pattern, RULES.tables[pattern], table, cols, tblFails);
        break; // Stop after first match
      }
    }
    
    // Check if no patterns matched
    if (!matchedPattern) {
      log.warn(`No rule pattern found for table ${table}`);
      console.log(`⚠️  No validation rules found for ${table}`);
      noRuleTables.push(table);
    }

    // 3. Run candidate_duplicates test for tables matching candidates* pattern
    if (tableShouldMatchPattern(rawName, 'candidates*')) {
      log.info(`Running candidate_duplicates test for ${table}`);
      try {
        if (!(await candidateDuplicates(table))) {
          log.error(`${table}: Failed candidate_duplicates test`);
          tblFails.push('candidate_duplicates: duplicate atlas_person_ids within same atlas_project_id');
        }
      } catch (error) {
        log.warn(`Error running candidate_duplicates test for ${table}: ${error}`);
      }
    }

    // Store failures for this table
    if (tblFails.length > 0) {
      failures[table] = tblFails;
      console.log(`❌ COMPLETED validation for ${table} with ${tblFails.length} errors`);
      // Log a summary of the failures for this table
      console.log(`   Issues: ${tblFails.join(', ')}`);
    } else {
      console.log(`✅ COMPLETED validation for ${table} with no errors`);
    }
  }

  // Generate final report
  if (noRuleTables.length > 0) {
    log.warn(
      `⚠️  ${noRuleTables.length} table(s) had no table-specific rules: ${noRuleTables.join(', ')}`
    );
  }

  const unusedPatterns = Object.keys(RULES.tables).filter(p => !usedPatterns.has(p));
  if (unusedPatterns.length > 0) {
    log.warn(
      `⚠️  ${unusedPatterns.length} rule pattern(s) were never matched: ${unusedPatterns.sort().join(', ')}`
    );
  }
  
  return failures;
}

// Helper function to check if table matches pattern with different strategies
function tableShouldMatchPattern(tableName: string, pattern: string): boolean {
  // Strategy 1: Direct minimatch
  const matchesDirectly = minimatch(tableName, pattern, { noglobstar: false, dot: true });
  
  // Strategy 2: Try exact match (without wildcards)
  const basePattern = pattern.replace(/\*$/, '');
  const matchesExactly = tableName === basePattern;
  
  // Strategy 3: For "people_salaries*" to match "people_salaries_xyz"
  const patternWithUnderscore = pattern.endsWith('*') && !pattern.endsWith('_*') 
    ? pattern.replace(/\*$/, '_*') 
    : null;
  const matchesWithUnderscore = patternWithUnderscore 
    ? minimatch(tableName, patternWithUnderscore, { noglobstar: false, dot: true })
    : false;
  
  // Strategy 4: For "people_references" to match "people_references_xyz"
  const patternWithWildcard = !pattern.includes('*') 
    ? `${pattern}_*` 
    : null;
  const matchesWithWildcard = patternWithWildcard
    ? minimatch(tableName, patternWithWildcard, { noglobstar: false, dot: true })
    : false;
  
  // Log all strategies for debugging
  log.info(`  Pattern "${pattern}" matching "${tableName}":
    - Direct minimatch: ${matchesDirectly}
    - Exact match: ${matchesExactly}
    - With underscore (${patternWithUnderscore || 'n/a'}): ${matchesWithUnderscore}
    - With wildcard (${patternWithWildcard || 'n/a'}): ${matchesWithWildcard}`);
  
  return matchesDirectly || matchesExactly || matchesWithUnderscore || matchesWithWildcard;
}
