import { pulseJobsDdl } from '../../../src/store/ddl';
import { quoteTableName } from '../../../src/store/table';
import { createTestDb, TestDb } from '../../helpers/pg.helper';

describe('quoteTableName', () => {
  test('quotes a bare table name', () => {
    expect(quoteTableName('pulse_jobs')).toBe('"pulse_jobs"');
  });

  test('quotes each part of schema.table', () => {
    expect(quoteTableName('jobs.pulse_jobs')).toBe('"jobs"."pulse_jobs"');
  });

  test.each(['', 'a.b.c', 'pulse jobs', 'jobs; DROP TABLE x', '"quoted"', '1jobs', 'jobs.'])(
    'rejects %p',
    (name) => {
      expect(() => quoteTableName(name)).toThrow(/Invalid table name/);
    }
  );
});

describe('pulseJobsDdl', () => {
  let db: TestDb;

  beforeAll(async () => {
    db = await createTestDb();
    await db.pool.query(pulseJobsDdl(db.table));
  });

  afterAll(async () => {
    await db.cleanup();
  });

  test('is idempotent', async () => {
    await expect(db.pool.query(pulseJobsDdl(db.table))).resolves.toBeDefined();
  });

  test('creates every column the mapper uses', async () => {
    const { rows } = await db.pool.query(
      `SELECT column_name, data_type FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = 'pulse_jobs' ORDER BY column_name`,
      [db.schema]
    );
    expect(Object.fromEntries(rows.map((r) => [r.column_name, r.data_type]))).toEqual({
      attempts: 'integer',
      backoff: 'jsonb',
      data: 'jsonb',
      disabled: 'boolean',
      end_date: 'timestamp with time zone',
      fail_count: 'integer',
      fail_reason: 'text',
      failed_at: 'timestamp with time zone',
      finished_count: 'integer',
      id: 'uuid',
      last_finished_at: 'timestamp with time zone',
      last_modified_by: 'text',
      last_run_at: 'timestamp with time zone',
      locked_at: 'timestamp with time zone',
      name: 'text',
      next_run_at: 'timestamp with time zone',
      priority: 'integer',
      progress: 'real',
      repeat_at: 'text',
      repeat_interval: 'text',
      repeat_timezone: 'text',
      result: 'jsonb',
      run_count: 'integer',
      should_save_result: 'boolean',
      skip_days: 'text',
      start_date: 'timestamp with time zone',
      type: 'text',
      unique_key: 'text',
    });
  });

  test('creates the lock, single, unique, finished and data indexes', async () => {
    const { rows } = await db.pool.query(
      `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = 'pulse_jobs' ORDER BY indexname`,
      [db.schema]
    );
    expect(rows.map((r) => r.indexname)).toEqual([
      'pulse_jobs_data_idx',
      'pulse_jobs_last_finished_at_idx',
      'pulse_jobs_lock_idx',
      'pulse_jobs_locked_at_idx',
      'pulse_jobs_pkey',
      'pulse_jobs_single_idx',
      'pulse_jobs_unique_key_idx',
    ]);
  });

  test('allows one single-type row per name but many normal rows', async () => {
    await db.pool.query(`INSERT INTO ${db.table} (name, type) VALUES ('n', 'normal'), ('n', 'normal'), ('s', 'single')`);
    await expect(db.pool.query(`INSERT INTO ${db.table} (name, type) VALUES ('s', 'single')`)).rejects.toThrow(
      /pulse_jobs_single_idx/
    );
  });

  test('generates uuid ids', async () => {
    const { rows } = await db.pool.query(`INSERT INTO ${db.table} (name, type) VALUES ('u', 'normal') RETURNING id`);
    expect(rows[0].id).toMatch(/^[0-9a-f-]{36}$/);
  });
});
