import { pulseJobsDdl } from '../../../src/store/ddl';
import { compileOrderBy, compileWhere, escapeLike, JobFilter, JobSort } from '../../../src/store/filter';
import { createTestDb, TestDb } from '../../helpers/pg.helper';

const HOUR = 60 * 60 * 1000;

describe('compileWhere', () => {
  let db: TestDb;
  const now = new Date();
  const past = new Date(now.getTime() - HOUR);
  const future = new Date(now.getTime() + HOUR);
  const ids: Record<string, string> = {};

  const names = async (filter: JobFilter): Promise<string[]> => {
    const params: unknown[] = [];
    const where = compileWhere(filter, params);
    const { rows } = await db.pool.query(`SELECT name FROM ${db.table} WHERE ${where} ORDER BY name`, params);
    return rows.map((row) => row.name);
  };

  beforeAll(async () => {
    db = await createTestDb();
    await db.pool.query(pulseJobsDdl(db.table));
    const insert = async (name: string, columns: Record<string, unknown>) => {
      const keys = ['name', 'type', ...Object.keys(columns)];
      const values = [name, 'normal', ...Object.values(columns)];
      const placeholders = keys.map((_, index) => `$${index + 1}`).join(', ');
      const { rows } = await db.pool.query(
        `INSERT INTO ${db.table} (${keys.join(', ')}) VALUES (${placeholders}) RETURNING id`,
        values
      );
      ids[name] = rows[0].id;
    };
    await insert('scheduled', { next_run_at: past });
    await insert('running', { next_run_at: past, locked_at: now });
    await insert('completed', { last_finished_at: now });
    await insert('failed', { last_finished_at: now, failed_at: now, next_run_at: future });
    await insert('recurring_100%_x', {
      repeat_interval: '5 minutes',
      disabled: true,
      next_run_at: future,
      data: JSON.stringify({ patientId: 'p1', kind: 'x' }),
    });
    await insert('recurring_2', { repeat_interval: '5 minutes', data: JSON.stringify({ patientId: 'p2' }) });
  });

  afterAll(async () => {
    await db.cleanup();
  });

  test('an empty filter matches every row', async () => {
    expect(await names({})).toHaveLength(6);
  });

  test('ids', async () => {
    expect(await names({ ids: [ids.running, ids.completed] })).toEqual(['completed', 'running']);
  });

  test('ids that are not uuids match nothing instead of raising a cast error', async () => {
    expect(await names({ ids: ['not-a-uuid', ids.running] })).toEqual(['running']);
    expect(await names({ ids: ['not-a-uuid'] })).toEqual([]);
  });

  test('name, as a string and as a list', async () => {
    expect(await names({ name: 'running' })).toEqual(['running']);
    expect(await names({ name: ['running', 'failed'] })).toEqual(['failed', 'running']);
    expect(await names({ name: [] })).toEqual([]);
  });

  test('nameNotIn', async () => {
    expect(await names({ nameNotIn: ['running', 'failed', 'completed', 'scheduled'] })).toEqual([
      'recurring_100%_x',
      'recurring_2',
    ]);
    expect(await names({ nameNotIn: [] })).toHaveLength(6);
  });

  test('nameContains is case-insensitive and treats % _ \\ literally', async () => {
    expect(await names({ nameContains: 'RUN' })).toEqual(['running']);
    expect(await names({ nameContains: '100%' })).toEqual(['recurring_100%_x']);
    expect(await names({ nameContains: '%' })).toEqual(['recurring_100%_x']);
    expect(await names({ nameContains: 'g_2' })).toEqual(['recurring_2']);
    expect(await names({ nameContains: 'g2' })).toEqual([]);
  });

  test.each([
    ['scheduled', ['failed', 'recurring_100%_x', 'scheduled']],
    ['running', ['running']],
    ['completed', ['completed']],
    ['failed', ['failed']],
  ] as const)('status %s', async (status, expected) => {
    expect(await names({ status })).toEqual(expected);
  });

  test('recurring and disabled', async () => {
    expect(await names({ recurring: true })).toEqual(['recurring_100%_x', 'recurring_2']);
    expect(await names({ recurring: false })).toHaveLength(4);
    expect(await names({ disabled: true })).toEqual(['recurring_100%_x']);
  });

  test('data containment', async () => {
    expect(await names({ data: { patientId: 'p1' } })).toEqual(['recurring_100%_x']);
    expect(await names({ data: { patientId: 'p1', kind: 'y' } })).toEqual([]);
  });

  test('date conditions combine with each other and with other fields', async () => {
    expect(await names({ nextRunAt: { isNull: true } })).toEqual(['completed', 'recurring_2']);
    expect(await names({ nextRunAt: { lte: now } })).toEqual(['running', 'scheduled']);
    expect(await names({ nextRunAt: { gt: now } })).toEqual(['failed', 'recurring_100%_x']);
    expect(await names({ lockedAt: { isNull: false } })).toEqual(['running']);
    expect(await names({ lastFinishedAt: { gte: now }, failedAt: { isNull: true } })).toEqual(['completed']);
    expect(await names({ status: 'scheduled', nextRunAt: { gt: now } })).toEqual(['failed', 'recurring_100%_x']);
  });

  test('ignores fields explicitly set to undefined', async () => {
    expect(await names({ name: undefined, status: undefined })).toHaveLength(6);
  });

  test.each([
    [{ _id: 'x' }, /Unknown job filter field "_id"/],
    [{ 'data.type': 'x' }, /Unknown job filter field "data.type"/],
    [{ nextRunAt: { $lte: new Date() } }, /Unknown date condition "\$lte" on "nextRunAt"/],
    [{ status: 'done' }, /Unknown job status "done"/],
    [{ status: 'constructor' }, /Unknown job status "constructor"/],
    [{ nextRunAt: { toString: new Date() } }, /Unknown date condition "toString" on "nextRunAt"/],
  ])('rejects leftover Mongo-style filter %p', (filter, message) => {
    expect(() => compileWhere(filter as unknown as JobFilter, [])).toThrow(message);
  });
});

describe('compileOrderBy', () => {
  test('maps fields to columns in the given order', () => {
    expect(compileOrderBy({ nextRunAt: 'asc', priority: 'desc' })).toBe('ORDER BY next_run_at ASC, priority DESC');
  });

  test('returns an empty string for no sort', () => {
    expect(compileOrderBy({})).toBe('');
  });

  test.each([
    [{ 'name; DROP TABLE x': 'asc' }, /Cannot sort jobs by/],
    [{ constructor: 'asc' }, /Cannot sort jobs by/],
    [{ nextRunAt: 1 }, /Invalid sort direction/],
  ])('rejects %p', (sort, message) => {
    expect(() => compileOrderBy(sort as unknown as JobSort)).toThrow(message);
  });
});

describe('escapeLike', () => {
  test('escapes LIKE metacharacters', () => {
    expect(escapeLike('a%b_c\\d')).toBe('a\\%b\\_c\\\\d');
  });
});
