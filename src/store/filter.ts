import { PulseError } from '../utils/error';

export type DateCond = { isNull?: boolean; lt?: Date; lte?: Date; gt?: Date; gte?: Date };
export type JobStatus = 'scheduled' | 'running' | 'completed' | 'failed';

export interface JobFilter {
  ids?: string[];
  name?: string | string[];
  nameNotIn?: string[];
  nameContains?: string;
  status?: JobStatus;
  recurring?: boolean;
  disabled?: boolean;
  data?: Record<string, unknown>;
  nextRunAt?: DateCond;
  lockedAt?: DateCond;
  lastFinishedAt?: DateCond;
  failedAt?: DateCond;
}

export type JobSortField = 'nextRunAt' | 'lastRunAt' | 'lastFinishedAt' | 'failedAt' | 'priority' | 'name';
export type JobSort = Partial<Record<JobSortField, 'asc' | 'desc'>>;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const STATUS_SQL: Record<JobStatus, string> = {
  running: 'locked_at IS NOT NULL',
  scheduled: 'locked_at IS NULL AND next_run_at IS NOT NULL',
  completed: 'last_finished_at IS NOT NULL AND failed_at IS NULL',
  failed: 'failed_at IS NOT NULL',
};

const DATE_COLUMNS = {
  nextRunAt: 'next_run_at',
  lockedAt: 'locked_at',
  lastFinishedAt: 'last_finished_at',
  failedAt: 'failed_at',
} as const;

const DATE_OPERATORS = { lt: '<', lte: '<=', gt: '>', gte: '>=' } as const;

const SORT_COLUMNS: Record<JobSortField, string> = {
  nextRunAt: 'next_run_at',
  lastRunAt: 'last_run_at',
  lastFinishedAt: 'last_finished_at',
  failedAt: 'failed_at',
  priority: 'priority',
  name: 'name',
};

const FILTER_FIELDS = new Set<string>([
  'ids',
  'name',
  'nameNotIn',
  'nameContains',
  'status',
  'recurring',
  'disabled',
  'data',
  ...Object.keys(DATE_COLUMNS),
]);

// Own-property lookup: `key in obj` or `obj[key]` would also accept inherited
// names such as "constructor" coming from a request.
const has = (object: object, key: string): boolean => Object.prototype.hasOwnProperty.call(object, key);

export const escapeLike = (text: string): string => text.replace(/[\\%_]/g, (char) => `\\${char}`);

/**
 * Compiles a filter into a SQL boolean expression, pushing its values onto
 * `params`. Unknown fields throw: an empty filter matches every row, so an
 * ignored field would silently widen cancel()/disable() to the whole table.
 */
export const compileWhere = (filter: JobFilter, params: unknown[]): string => {
  for (const key of Object.keys(filter)) {
    if (!FILTER_FIELDS.has(key)) {
      throw new PulseError(`Unknown job filter field "${key}"`);
    }
  }

  const clauses: string[] = [];
  const param = (value: unknown): string => {
    params.push(value);
    return `$${params.length}`;
  };

  if (filter.ids !== undefined) {
    clauses.push(`id = ANY(${param(filter.ids.filter((id) => UUID.test(id)))}::uuid[])`);
  }
  if (filter.name !== undefined) {
    clauses.push(
      Array.isArray(filter.name) ? `name = ANY(${param(filter.name)}::text[])` : `name = ${param(filter.name)}`
    );
  }
  if (filter.nameNotIn !== undefined) {
    clauses.push(`NOT (name = ANY(${param(filter.nameNotIn)}::text[]))`);
  }
  if (filter.nameContains !== undefined) {
    clauses.push(`name ILIKE ${param(`%${escapeLike(filter.nameContains)}%`)}`);
  }
  if (filter.status !== undefined) {
    if (!has(STATUS_SQL, filter.status)) {
      throw new PulseError(`Unknown job status "${filter.status}"`);
    }
    clauses.push(STATUS_SQL[filter.status]);
  }
  if (filter.recurring !== undefined) {
    clauses.push(filter.recurring ? 'repeat_interval IS NOT NULL' : 'repeat_interval IS NULL');
  }
  if (filter.disabled !== undefined) {
    clauses.push(`disabled = ${param(filter.disabled)}`);
  }
  if (filter.data !== undefined) {
    clauses.push(`data @> ${param(JSON.stringify(filter.data))}::jsonb`);
  }

  for (const [field, column] of Object.entries(DATE_COLUMNS) as Array<[keyof typeof DATE_COLUMNS, string]>) {
    const cond = filter[field];
    if (cond === undefined) {
      continue;
    }
    for (const [key, value] of Object.entries(cond)) {
      if (value === undefined) {
        continue;
      }
      if (key === 'isNull') {
        clauses.push(`${column} IS ${value ? '' : 'NOT '}NULL`);
      } else if (has(DATE_OPERATORS, key)) {
        clauses.push(`${column} ${DATE_OPERATORS[key as keyof typeof DATE_OPERATORS]} ${param(value)}`);
      } else {
        throw new PulseError(`Unknown date condition "${key}" on "${field}"`);
      }
    }
  }

  return clauses.length > 0 ? clauses.join(' AND ') : 'TRUE';
};

/** Compiles a sort through a column whitelist; caller input never reaches the SQL text. */
export const compileOrderBy = (sort: JobSort): string => {
  const parts = Object.entries(sort).map(([field, direction]) => {
    if (!has(SORT_COLUMNS, field)) {
      throw new PulseError(`Cannot sort jobs by "${field}"`);
    }
    const column = SORT_COLUMNS[field as JobSortField];
    if (direction !== 'asc' && direction !== 'desc') {
      throw new PulseError(`Invalid sort direction "${direction}" for "${field}"`);
    }
    return `${column} ${direction.toUpperCase()}`;
  });
  return parts.length > 0 ? `ORDER BY ${parts.join(', ')}` : '';
};
