import { settings } from './config.js';
import { log } from './logger.js';
import { fetchScalar } from './db.js';

(async () => {
  log.info(`Schema: ${settings.TARGET_SCHEMA}`);
  const one = await fetchScalar<number>('SELECT 1 AS value');
  log.info(`Redshift answered: ${one}`);
})();
