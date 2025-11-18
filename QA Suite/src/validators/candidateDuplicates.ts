import { fetchRows } from '../db.js';
import { log } from '../logger.js';
import { parseTableIdentifier, formatTableIdentifier } from './helpers.js';

/**
 * Validates that there are no duplicate atlas_person_ids for the same atlas_project_id
 * in candidate tables
 *
 * @param table Fully-qualified table name (e.g. "public.candidates_202505")
 * @returns true if no duplicates found, false otherwise
 */
export async function candidateDuplicates(
  table: string,
): Promise<boolean> {
  try {
    // Parse table identifier
    const { schema, table: tableName } = parseTableIdentifier(table);
    const formattedTable = formatTableIdentifier(schema, tableName);
    
    // Check if the required columns exist
    const columns = await fetchRows<{ column_name: string }>(`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      AND column_name IN ('atlas_person_id', 'atlas_project_id')
    `, [schema, tableName]);
    
    const hasPersonId = columns.some(c => c.column_name === 'atlas_person_id');
    const hasProjectId = columns.some(c => c.column_name === 'atlas_project_id');
    
    if (!hasPersonId || !hasProjectId) {
      log.warn(`${table}: Missing required columns for candidate duplicates check (atlas_person_id and/or atlas_project_id)`);
      return true; // Skip validation if columns don't exist
    }
    
    // Find duplicate atlas_person_ids within the same atlas_project_id
    const duplicates = await fetchRows<{ 
      atlas_project_id: string;
      atlas_person_id: string;
      count: string;
    }>(`
      SELECT 
        atlas_project_id::text,
        atlas_person_id::text,
        COUNT(*) AS count
      FROM ${formattedTable}
      WHERE atlas_person_id IS NOT NULL 
        AND atlas_project_id IS NOT NULL
      GROUP BY atlas_project_id, atlas_person_id
      HAVING COUNT(*) > 1
      ORDER BY count DESC
      LIMIT 10
    `);
    
    if (duplicates.length > 0) {
      // Format examples for the error message
      const examples = duplicates
        .map(d => `project_id=${d.atlas_project_id}, person_id=${d.atlas_person_id} (${d.count}×)`)
        .join('; ');
      
      log.error(`${table}: Found ${duplicates.length} cases of duplicate atlas_person_ids within the same atlas_project_id – e.g. ${examples}`);
      return false;
    }
    
    return true;
  } catch (error) {
    log.error(`Error in candidateDuplicates for ${table}: ${error}`);
    return false;
  }
}

export {}; 