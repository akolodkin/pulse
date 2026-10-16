import type { JobAttributes } from '../job';

type ColumnKind = 'value' | 'date' | 'json';

interface ColumnSpec {
  attr: string;
  column: string;
  kind: ColumnKind;
  /** NOT NULL column with a default: an undefined attribute is treated as absent. */
  hasDefault?: boolean;
}

const COLUMNS: readonly ColumnSpec[] = [
  { attr: 'name', column: 'name', kind: 'value' },
  { attr: 'type', column: 'type', kind: 'value' },
  { attr: 'priority', column: 'priority', kind: 'value', hasDefault: true },
  { attr: 'disabled', column: 'disabled', kind: 'value', hasDefault: true },
  { attr: 'data', column: 'data', kind: 'json' },
  { attr: 'result', column: 'result', kind: 'json' },
  { attr: 'backoff', column: 'backoff', kind: 'json' },
  { attr: 'nextRunAt', column: 'next_run_at', kind: 'date' },
  { attr: 'lockedAt', column: 'locked_at', kind: 'date' },
  { attr: 'lastRunAt', column: 'last_run_at', kind: 'date' },
  { attr: 'lastFinishedAt', column: 'last_finished_at', kind: 'date' },
  { attr: 'failedAt', column: 'failed_at', kind: 'date' },
  { attr: 'startDate', column: 'start_date', kind: 'date' },
  { attr: 'endDate', column: 'end_date', kind: 'date' },
  { attr: 'repeatInterval', column: 'repeat_interval', kind: 'value' },
  { attr: 'repeatTimezone', column: 'repeat_timezone', kind: 'value' },
  { attr: 'repeatAt', column: 'repeat_at', kind: 'value' },
  { attr: 'skipDays', column: 'skip_days', kind: 'value' },
  { attr: 'failReason', column: 'fail_reason', kind: 'value' },
  { attr: 'lastModifiedBy', column: 'last_modified_by', kind: 'value' },
  { attr: 'runCount', column: 'run_count', kind: 'value' },
  { attr: 'finishedCount', column: 'finished_count', kind: 'value' },
  { attr: 'failCount', column: 'fail_count', kind: 'value' },
  { attr: 'attempts', column: 'attempts', kind: 'value' },
  { attr: 'progress', column: 'progress', kind: 'value' },
  { attr: 'shouldSaveResult', column: 'should_save_result', kind: 'value', hasDefault: true },
];

export type JobRecord = Omit<JobAttributes, 'pulse'>;
export type ColumnValues = Array<[column: string, value: unknown]>;

const toDb = (kind: ColumnKind, value: unknown): unknown => {
  if (kind === 'date') {
    return new Date(value as Date | number | string);
  }
  if (kind === 'json') {
    // Stringified explicitly: pg would turn a JS array into a Postgres array, not jsonb.
    return JSON.stringify(value);
  }
  return value;
};

/**
 * Columns to write for a job. Only attributes present on the object are
 * included, like a partial update: absent attributes keep their stored value.
 */
export const toColumnValues = (attrs: Partial<JobAttributes>): ColumnValues => {
  const source = attrs as Record<string, unknown>;
  const values: ColumnValues = [];
  for (const spec of COLUMNS) {
    if (!Object.prototype.hasOwnProperty.call(source, spec.attr)) {
      continue;
    }
    const value = source[spec.attr];
    if (value === undefined || value === null) {
      if (!spec.hasDefault) {
        values.push([spec.column, null]);
      }
      continue;
    }
    values.push([spec.column, toDb(spec.kind, value)]);
  }
  return values;
};

/** Builds job attributes from a row; NULL columns are left out. */
export const fromRow = (row: Record<string, unknown>): JobRecord => {
  const record: Record<string, unknown> = { _id: row.id };
  for (const spec of COLUMNS) {
    const value = row[spec.column];
    if (value !== null && value !== undefined) {
      record[spec.attr] = value;
    }
  }
  return record as unknown as JobRecord;
};
