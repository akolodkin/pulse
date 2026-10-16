import { quoteTableName } from './table';

/**
 * Canonical schema for the jobs table. Consumers that manage their own
 * migrations (LISA uses Drizzle) mirror this; `ensureSchema()` runs it as-is.
 */
export const pulseJobsDdl = (table: string): string => {
  const t = quoteTableName(table);
  const base = table.split('.').pop() as string;
  const index = (suffix: string) => `"${base}_${suffix}"`;

  return `
CREATE TABLE IF NOT EXISTS ${t} (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  type text NOT NULL,
  priority integer NOT NULL DEFAULT 0,
  disabled boolean NOT NULL DEFAULT false,
  data jsonb,
  result jsonb,
  backoff jsonb,
  next_run_at timestamptz,
  locked_at timestamptz,
  last_run_at timestamptz,
  last_finished_at timestamptz,
  failed_at timestamptz,
  start_date timestamptz,
  end_date timestamptz,
  repeat_interval text,
  repeat_timezone text,
  repeat_at text,
  skip_days text,
  fail_reason text,
  last_modified_by text,
  run_count integer,
  finished_count integer,
  fail_count integer,
  attempts integer,
  progress real,
  should_save_result boolean NOT NULL DEFAULT false,
  unique_key text
);
CREATE INDEX IF NOT EXISTS ${index('lock_idx')} ON ${t} (name, next_run_at, priority DESC) WHERE locked_at IS NULL AND NOT disabled;
CREATE INDEX IF NOT EXISTS ${index('locked_at_idx')} ON ${t} (locked_at) WHERE locked_at IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ${index('single_idx')} ON ${t} (name) WHERE type = 'single';
CREATE UNIQUE INDEX IF NOT EXISTS ${index('unique_key_idx')} ON ${t} (name, unique_key) WHERE unique_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ${index('last_finished_at_idx')} ON ${t} (last_finished_at);
CREATE INDEX IF NOT EXISTS ${index('data_idx')} ON ${t} USING gin (data jsonb_path_ops);
`;
};
