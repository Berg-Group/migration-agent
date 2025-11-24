// Export all validators from their respective files
export * from './notNull.js';
export * from './unique.js';
export * from './uniqueCi.js';
export * from './acceptedValues.js';
export * from './matchesRegex.js';
export * from './matchesIso8601.js';
export * from './matchesDateYmd.js';
export * from './warnIfNullFraction.js';
export * from './errorIfNullFraction.js';
export * from './columnIsConstant.js';
export * from './noHtml.js';
export * from './mustExist.js';
export * from './consecutivePositions.js';
export * from './candidateDuplicates.js';
export * from './email.js';
export * from './currency.js';
export * from './numeric.js';
export * from './url.js';
export * from './trim.js';
export * from './locationCoverage.js';

// Boolean-specific validators
export * from './isBoolean.js';
export * from './booleanIs.js';
export * from './booleanIsMixed.js';

// Export UUID regex for validation
export const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
