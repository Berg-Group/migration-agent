// src/config.ts
import dotenv from 'dotenv';
import { z } from 'zod';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';
import { readFileSync } from 'fs';
import yaml from 'js-yaml';

// Load .env from parent directory (Migration agent folder)
// Get the directory where this config.ts file is located
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Go up from src/ to QA Suite/, then to Migration agent/
const envPath = resolve(__dirname, '../../.env');
const migrationConfigPath = resolve(__dirname, '../../migration_config.yml');

dotenv.config({ path: envPath });

// Load migration config YAML
const migrationConfigContent = readFileSync(migrationConfigPath, 'utf8');
const migrationConfig = yaml.load(migrationConfigContent) as any;

// Get TARGET_SCHEMA from migration_config.yml
const TARGET_SCHEMA = migrationConfig?.target_schema;

if (!TARGET_SCHEMA) {
  throw new Error('target_schema not found in migration_config.yml');
}

/** ❶ describe every required env-var */
const SettingsSchema = z.object({
  REDSHIFT_USER: z.string(),
  REDSHIFT_PASSWORD: z.string(),
  REDSHIFT_HOST: z.string(),
  REDSHIFT_PORT: z.coerce.number().default(5439),   // coerce "5439" → 5439
  REDSHIFT_DB: z.string(),

  TARGET_SCHEMA: z.string().default(TARGET_SCHEMA),
});

export type Settings = z.infer<typeof SettingsSchema> & {
  TABLE_PREFIXES: string[];
};

// Parse base settings from environment
let baseSettings = SettingsSchema.parse(process.env);

// Export mutable settings object with empty TABLE_PREFIXES by default
export let settings: Settings = { 
  ...baseSettings,
  TABLE_PREFIXES: []
};

/**
 * Set table prefixes from command-line arguments
 * This allows running: npm run qa people companies
 */
export function overrideTablePrefixes(prefixes: string[]) {
  if (prefixes.length > 0) {
    settings = {
      ...settings,
      TABLE_PREFIXES: prefixes
    };
  }
}
