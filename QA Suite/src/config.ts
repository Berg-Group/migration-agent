// src/config.ts
import 'dotenv/config';
import { z } from 'zod';

/** ❶ describe every required env-var */
const SettingsSchema = z.object({
  REDSHIFT_USER: z.string(),
  REDSHIFT_PASSWORD: z.string(),
  REDSHIFT_HOST: z.string(),
  REDSHIFT_PORT: z.coerce.number().default(5439),   // coerce "5439" → 5439
  REDSHIFT_DB: z.string(),

  TARGET_SCHEMA: z.string(),

  // "" → [];  "people,orders" → ["people","orders"]
  TABLE_PREFIXES: z
    .string()
    .default('')
    .transform((s) =>
      s
        .split(',')
        .map((x) => x.trim())
        .filter(Boolean),
    ),
    
  // List of table prefixes to exclude from testing
  EXCLUDED_TABLE_PREFIXES: z
    .string()
    .default('')
    .transform((s) =>
      s
        .split(',')
        .map((x) => x.trim())
        .filter(Boolean),
    ),
});

export type Settings = z.infer<typeof SettingsSchema>;

// Parse base settings from environment
let baseSettings = SettingsSchema.parse(process.env);

// Export mutable settings object
export let settings = { ...baseSettings };

/**
 * Override table prefixes from command-line arguments
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
