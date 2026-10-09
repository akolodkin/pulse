import { PgJobStore } from '../../../src/store';
import { createTestDb, TestDb } from '../../helpers/pg.helper';

describe('PgJobStore writes and reads', () => {
  let db: TestDb;
  let store: PgJobStore;
  const soon = () => new Date(Date.now() + 60_000);

  beforeAll(async () => {
    db = await createTestDb();
    store = new PgJobStore(db.pool, db.table);
    await store.ensureSchema();
  });

  beforeEach(async () => {
    await db.pool.query(`TRUNCATE ${db.table}`);
  });

  afterAll(async () => {
    await db.cleanup();
  });

  test('rejects an invalid table name up front', () => {
    expect(() => new PgJobStore(db.pool, 'jobs; DROP TABLE x')).toThrow(/Invalid table name/);
  });

  test('insert returns the new id and nextRunAt; find reads the row back', async () => {
    const nextRunAt = soon();
    const saved = await store.insert([
      ['name', 'a'],
      ['type', 'normal'],
      ['next_run_at', nextRunAt],
      ['data', '{"x":1}'],
    ]);
    expect(saved._id).toMatch(/^[0-9a-f-]{36}$/);
    expect(saved.nextRunAt).toEqual(nextRunAt);

    const [record] = await store.find({ ids: [saved._id] });
    expect(record).toEqual({
      _id: saved._id,
      name: 'a',
      type: 'normal',
      priority: 0,
      disabled: false,
      shouldSaveResult: false,
      nextRunAt,
      data: { x: 1 },
    });
  });

  test('update writes only the given columns', async () => {
    const lockedAt = new Date();
    const { _id } = await store.insert([
      ['name', 'a'],
      ['type', 'normal'],
      ['locked_at', lockedAt],
      ['run_count', 3],
    ]);
    await store.update(_id, [['fail_reason', 'boom']]);
    const [record] = await store.find({ ids: [_id] });
    expect(record.lockedAt).toEqual(lockedAt);
    expect(record.runCount).toBe(3);
    expect(record.failReason).toBe('boom');
  });

  test('update of a deleted job returns undefined instead of throwing', async () => {
    const { _id } = await store.insert([
      ['name', 'a'],
      ['type', 'normal'],
    ]);
    await store.delete({ ids: [_id] });
    await expect(store.update(_id, [['name', 'a']])).resolves.toBeUndefined();
  });

  test('upsertSingle keeps one row per name and does not clobber columns it was not given', async () => {
    const first = await store.upsertSingle(
      [
        ['name', 'tick'],
        ['type', 'single'],
        ['repeat_interval', '1 minute'],
      ],
      false
    );
    const lockedAt = new Date();
    await store.update(first._id, [
      ['locked_at', lockedAt],
      ['run_count', 7],
    ]);

    const second = await store.upsertSingle(
      [
        ['name', 'tick'],
        ['type', 'single'],
        ['repeat_interval', '5 minutes'],
      ],
      false
    );

    expect(second._id).toBe(first._id);
    const records = await store.find({ name: 'tick' });
    expect(records).toHaveLength(1);
    expect(records[0].repeatInterval).toBe('5 minutes');
    expect(records[0].lockedAt).toEqual(lockedAt);
    expect(records[0].runCount).toBe(7);
  });

  test('upsertSingle with keepExistingNextRunAt leaves an existing next_run_at but sets it on insert', async () => {
    const original = soon();
    const inserted = await store.upsertSingle(
      [
        ['name', 'tick'],
        ['type', 'single'],
        ['next_run_at', original],
      ],
      true
    );
    expect(inserted.nextRunAt).toEqual(original);

    const updated = await store.upsertSingle(
      [
        ['name', 'tick'],
        ['type', 'single'],
        ['next_run_at', new Date(0)],
      ],
      true
    );
    expect(updated.nextRunAt).toEqual(original);
  });

  test('upsertUnique updates the row with the same key, or keeps it when insertOnly', async () => {
    const base: Array<[string, unknown]> = [
      ['name', 'u'],
      ['type', 'normal'],
      ['unique_key', '{"patientId":"p1"}'],
    ];
    const first = await store.upsertUnique([...base, ['data', '{"v":1}']], false);
    const second = await store.upsertUnique([...base, ['data', '{"v":2}']], false);
    expect(second._id).toBe(first._id);
    expect((await store.find({ name: 'u' }))[0].data).toEqual({ v: 2 });

    const third = await store.upsertUnique([...base, ['data', '{"v":3}']], true);
    expect(third._id).toBe(first._id);
    expect((await store.find({ name: 'u' }))[0].data).toEqual({ v: 2 });

    const other = await store.upsertUnique(
      [
        ['name', 'u'],
        ['type', 'normal'],
        ['unique_key', '{"patientId":"p2"}'],
      ],
      true
    );
    expect(other._id).not.toBe(first._id);
  });

  test('find applies sort, limit and skip', async () => {
    const base = Date.now();
    for (const [name, offset] of [
      ['a', 1],
      ['b', 2],
      ['c', 3],
    ] as const) {
      await store.insert([
        ['name', name],
        ['type', 'normal'],
        ['next_run_at', new Date(base + offset * 1000)],
      ]);
    }
    const page = await store.find({}, { sort: { nextRunAt: 'desc' }, limit: 2, skip: 1 });
    expect(page.map((r) => r.name)).toEqual(['b', 'a']);
  });

  test('count, delete and setDisabled report affected rows', async () => {
    for (const name of ['a', 'b', 'c']) {
      await store.insert([
        ['name', name],
        ['type', 'normal'],
      ]);
    }
    expect(await store.count()).toBe(3);
    expect(await store.setDisabled({ name: ['a', 'b'] }, true)).toBe(2);
    expect(await store.setDisabled({ name: ['a', 'b'] }, true)).toBe(0);
    expect(await store.count({ disabled: true })).toBe(2);
    expect(await store.delete({ name: 'a' })).toBe(1);
    expect(await store.count()).toBe(2);
  });

  describe('destructive calls refuse undefined filter values', () => {
    beforeEach(async () => {
      for (const name of ['a', 'b']) {
        await store.insert([
          ['name', name],
          ['type', 'normal'],
        ]);
      }
    });

    test('delete({ name: undefined }) rejects and deletes nothing', async () => {
      await expect(store.delete({ name: undefined })).rejects.toThrow(
        'Refusing to delete jobs: filter field "name" is undefined'
      );
      expect(await store.count()).toBe(2);
    });

    test('delete rejects an undefined value inside data', async () => {
      await expect(store.delete({ name: 'a', data: { orderId: undefined } })).rejects.toThrow(
        'filter field "data.orderId" is undefined'
      );
      expect(await store.count()).toBe(2);
    });

    test('setDisabled rejects an undefined date operator', async () => {
      await expect(store.setDisabled({ nextRunAt: { lte: undefined } }, true)).rejects.toThrow(
        'Refusing to update jobs: filter field "nextRunAt.lte" is undefined'
      );
      expect(await store.count({ disabled: true })).toBe(0);
    });

    test('find({ name: undefined }) still reads every row', async () => {
      expect(await store.find({ name: undefined })).toHaveLength(2);
    });
  });

  test('unlock clears locked_at for the given ids only', async () => {
    const now = new Date();
    const a = await store.insert([
      ['name', 'a'],
      ['type', 'normal'],
      ['locked_at', now],
    ]);
    const b = await store.insert([
      ['name', 'b'],
      ['type', 'normal'],
      ['locked_at', now],
    ]);
    await store.unlock([a._id]);
    await store.unlock([]);
    expect((await store.find({ ids: [a._id] }))[0].lockedAt).toBeUndefined();
    expect((await store.find({ ids: [b._id] }))[0].lockedAt).toEqual(now);
  });
});
