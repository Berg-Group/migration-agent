// Check database connection and configuration
import { Pool } from 'pg';
import { settings } from './config.js';
import { log } from './logger.js';

// Main function to test database connection
async function checkConnection() {
  log.info('=== Database Connection Test ===');
  log.info(`Host: ${settings.REDSHIFT_HOST}`);
  log.info(`Port: ${settings.REDSHIFT_PORT}`);
  log.info(`Database: ${settings.REDSHIFT_DB}`);
  log.info(`User: ${settings.REDSHIFT_USER}`);
  log.info(`Target Schema: ${settings.TARGET_SCHEMA}`);
  
  // Create pool with connection info from settings
  const pool = new Pool({
    host: settings.REDSHIFT_HOST,
    port: settings.REDSHIFT_PORT,
    database: settings.REDSHIFT_DB,
    user: settings.REDSHIFT_USER,
    password: settings.REDSHIFT_PASSWORD,
    // Set shorter connection timeout for faster feedback
    connectionTimeoutMillis: 5000,
  });
  
  try {
    log.info('Attempting to connect to database...');
    const client = await pool.connect();
    log.info('✅ Successfully connected to database');
    
    // Test basic query
    try {
      log.info('Testing basic query...');
      const res = await client.query('SELECT current_database() AS db, current_user AS user');
      log.info(`✅ Query successful: Connected to database ${res.rows[0].db} as ${res.rows[0].user}`);
      
      // Test schema exists
      try {
        log.info(`Testing if schema "${settings.TARGET_SCHEMA}" exists...`);
        const schemaRes = await client.query(`
          SELECT schema_name 
          FROM information_schema.schemata 
          WHERE schema_name = $1
        `, [settings.TARGET_SCHEMA]);
        
        if (schemaRes.rows.length > 0) {
          log.info(`✅ Schema "${settings.TARGET_SCHEMA}" exists`);
          
          // Test if we can list tables in the schema
          try {
            log.info(`Listing tables in schema "${settings.TARGET_SCHEMA}"...`);
            const tablesRes = await client.query(`
              SELECT table_name 
              FROM information_schema.tables 
              WHERE table_schema = $1
              LIMIT 10
            `, [settings.TARGET_SCHEMA]);
            
            if (tablesRes.rows.length > 0) {
              log.info(`✅ Found ${tablesRes.rows.length} tables in schema "${settings.TARGET_SCHEMA}"`);
              tablesRes.rows.forEach((row, i) => {
                log.info(`   ${i+1}. ${row.table_name}`);
              });
              
              // Test a query on one of the tables
              if (tablesRes.rows.length > 0) {
                const testTable = tablesRes.rows[0].table_name;
                log.info(`Testing a query on table "${testTable}"...`);
                try {
                  const tableQuery = await client.query(`
                    SELECT * FROM "${settings.TARGET_SCHEMA}"."${testTable}" LIMIT 1
                  `);
                  log.info(`✅ Successfully queried table "${testTable}"`);
                } catch (tableQueryErr: any) {
                  log.error(`❌ Error querying table "${testTable}": ${tableQueryErr.message}`);
                }
              }
            } else {
              log.error(`❌ No tables found in schema "${settings.TARGET_SCHEMA}"`);
            }
          } catch (tablesErr: any) {
            log.error(`❌ Error listing tables: ${tablesErr.message}`);
          }
        } else {
          log.error(`❌ Schema "${settings.TARGET_SCHEMA}" does not exist`);
        }
      } catch (schemaErr: any) {
        log.error(`❌ Error checking schema: ${schemaErr.message}`);
      }
    } catch (queryErr: any) {
      log.error(`❌ Error executing basic query: ${queryErr.message}`);
    } finally {
      client.release();
    }
  } catch (connErr: any) {
    log.error(`❌ Connection failed: ${connErr.message}`);
    log.error('  Check that:');
    log.error('  1. The database credentials in your .env file are correct');
    log.error('  2. The database server is running and accessible');
    log.error('  3. Network/firewall settings allow the connection');
  } finally {
    // Close pool
    await pool.end();
    log.info('=== Connection test complete ===');
  }
}

// Run the test
checkConnection().catch(err => {
  log.error(`Unhandled error: ${err.message}`);
  process.exit(1);
}); 