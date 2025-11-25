import { fetchRows } from '../db.js';
import { log } from '../logger.js';

/**
 * Checks if positions for each atlas_attribute_id are consecutive from 1 to n with no gaps
 * This is important for option ordering in custom attribute options
 * 
 * @param table Fully-qualified table name (e.g. "public.custom_attribute_options")
 * @param attributeIdColumn Column that contains the attribute ID 
 * @param positionColumn Column that contains the position value
 * @returns true if all attribute groups have consecutive positions from 1 to n
 */
export async function consecutiveAttributePositions(
  table: string,
  attributeIdColumn: string = 'atlas_attribute_id',
  positionColumn: string = 'position'
): Promise<boolean> {
  try {
    // Get all rows with attribute ID and position
    const rows = await fetchRows<{ attribute_id: string, position: number }>(
      `
      SELECT 
        ${attributeIdColumn} as attribute_id,
        ${positionColumn}::int as position
      FROM ${table}
      WHERE ${attributeIdColumn} IS NOT NULL 
        AND ${positionColumn} IS NOT NULL
      ORDER BY ${attributeIdColumn}, ${positionColumn}::int
      `
    );

    if (rows.length === 0) {
      log.warn(`${table}: No rows found with ${attributeIdColumn} and ${positionColumn}`);
      return true; // No rows to validate
    }

    log.info(`${table}: Found ${rows.length} rows with position values`);
    
    // Group by attribute_id
    const attributeGroups = new Map<string, number[]>();
    
    // Collect all positions for each attribute ID
    for (const row of rows) {
      if (!attributeGroups.has(row.attribute_id)) {
        attributeGroups.set(row.attribute_id, []);
      }
      attributeGroups.get(row.attribute_id)!.push(row.position);
    }
    
    log.info(`${table}: Found ${attributeGroups.size} unique attribute IDs`);
    
    // Check each attribute group
    let hasIssues = false;
    
    for (const [attrId, positions] of attributeGroups.entries()) {
      // Sort positions in ascending order
      positions.sort((a, b) => a - b);
      
      // Log the positions for debugging
      if (positions.length <= 10) {
        log.info(`Attribute ${attrId}: positions = [${positions.join(', ')}]`);
      } else {
        log.info(`Attribute ${attrId}: ${positions.length} positions, range ${positions[0]}-${positions[positions.length-1]}`);
      }
      
      // Check if first position is 1
      const startsAt1 = positions[0] === 1;
      if (!startsAt1) {
        hasIssues = true;
        log.error(`${table}: Attribute ${attrId} positions don't start at 1 (starts at ${positions[0]})`);
      }
      
      // Check for consecutiveness
      let isConsecutive = true;
      let missingPositions = [];
      
      for (let i = 0; i < positions.length - 1; i++) {
        const current = positions[i];
        const next = positions[i + 1];
        
        if (next - current > 1) {
          isConsecutive = false;
          
          // Add missing positions to the list
          for (let j = current + 1; j < next; j++) {
            missingPositions.push(j);
          }
        }
      }
      
      // Report gaps
      if (!isConsecutive) {
        hasIssues = true;
        
        if (missingPositions.length <= 5) {
          log.error(`${table}: Attribute ${attrId} is missing positions: ${missingPositions.join(', ')}`);
        } else {
          log.error(`${table}: Attribute ${attrId} is missing ${missingPositions.length} positions (not consecutive)`);
        }
      }
      
      // Report success
      if (startsAt1 && isConsecutive) {
        log.info(`${table}: Attribute ${attrId} has valid consecutive positions 1 through ${positions.length}`);
      }
    }
    
    return !hasIssues;
  } catch (error: any) {
    log.error(`Error validating consecutive positions in ${table}: ${error.message}`);
    return false;
  }
} 