// src/cli.ts - Comprehensive validation using tableTests
import { log } from './logger.js';
import { runColumnTests } from './tests/tableTests.js';
import { settings, overrideTablePrefixes } from './config.js';

(async () => {
  // Parse command-line arguments (skip first two: node and script path)
  const cliArgs = process.argv.slice(2);
  
  // If arguments are provided, use them as table prefixes
  if (cliArgs.length > 0) {
    overrideTablePrefixes(cliArgs);
  }
  
  // Always display start message, even in quiet mode
  console.log("\n=================================================");
  console.log(` 🚀 Starting QA validation for ${settings.TARGET_SCHEMA}`);
  console.log(`    Table Prefixes: ${settings.TABLE_PREFIXES.length > 0 ? settings.TABLE_PREFIXES.join(', ') : 'All tables'}`);
  console.log("=================================================\n");
  
  log.info(`Starting QA tests for schema: ${settings.TARGET_SCHEMA}`);
  
  try {
    // Run the comprehensive tests from tableTests.js
    const failures = await runColumnTests();
    
    // Process and display failures
    const failingTables = Object.keys(failures);
    
    // Always display completion summary, even in quiet mode
    console.log("\n=================================================");
    if (failingTables.length === 0) {
      console.log(` 🎉 SUCCESS: All tables passed validation!`);
      log.info('🎉 All QA checks passed!');
    } else {
      console.log(` ❌ FAILED: ${failingTables.length} of ${(await listTables()).length} tables failed validation`);
      // List failing tables briefly
      if (failingTables.length <= 10) {
        console.log(`    Failed tables: ${failingTables.join(', ')}`);
      } else {
        console.log(`    Failed tables: ${failingTables.slice(0, 10).join(', ')} ... and ${failingTables.length - 10} more`);
      }
      log.error(`❌ ${failingTables.length} tables failed validation checks`);
    }
    console.log(`    Log file: ${log.getLogFilePath()}`);
    console.log("=================================================\n");
    
    log.info(`Full test results saved to: ${log.getLogFilePath()}`);
    
    // Exit with appropriate code
    process.exit(failingTables.length === 0 ? 0 : 1);
  } catch (error: any) {
    console.log("\n=================================================");
    console.log(` ❌ ERROR: Validation process failed`);
    console.log(`    ${error.message}`);
    console.log(`    Log file: ${log.getLogFilePath()}`);
    console.log("=================================================\n");
    
    log.error(`Error running QA tests: ${error.message}`);
    if (error.stack) {
      log.error(error.stack);
    }
    log.info(`Log file saved to: ${log.getLogFilePath()}`);
    process.exit(1);
  }
})();

async function listTables() {
  // Importing directly to avoid circular dependencies
  const { fetchRows } = await import('./db.js');
  const { settings } = await import('./config.js');
  
  const query = `
    SELECT COUNT(*) as count
    FROM information_schema.tables
    WHERE table_schema = $1
  `;
  
  const result = await fetchRows(query, [settings.TARGET_SCHEMA]);
  return result[0].count;
}
