import { PulseError } from '../utils/error';

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Validates a `table` or `schema.table` name and returns it double-quoted.
 * The name is interpolated into every statement, so it is never taken as-is.
 */
export const quoteTableName = (table: string): string => {
  const parts = table.split('.');
  if (parts.length > 2 || !parts.every((part) => IDENTIFIER.test(part))) {
    throw new PulseError(
      `Invalid table name "${table}": expected "table" or "schema.table" made of letters, digits and underscores`
    );
  }
  return parts.map((part) => `"${part}"`).join('.');
};
