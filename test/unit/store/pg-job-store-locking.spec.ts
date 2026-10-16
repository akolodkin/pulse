import { Pool } from 'pg';
import { PgJobStore } from '../../../src/store';
import { createTestDb, TestDb, testDatabaseUrl } from '../../helpers/pg.helper';

const MINUTE = 60_000;

describe('PgJobStore locking', () => {
  let db: TestDb;
  let store: PgJobStore;
  const sort = { nextRunAt: 'asc', priority: 'desc' } as const;

  const insertJob = (values: Array<[string, unknown]>) => store.insert([['type', 'normal'], ...values]);
  const lockOptions = (now = new Date()) => ({
    now,
    nextScanAt: new Date(now.getTime() + 5000),
    lockDeadline: new Date(now.getTime() - 10 * MINUTE),
    sort,
  });

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

  describe('lockNext', () => {
    test('locks the earliest due job of that name, then the next one', async () => {
      const now = new Date();
      await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() - 1000)],
        ['data', '{"n":2}'],
      ]);
      await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() - 2000)],
        ['data', '{"n":1}'],
      ]);
      await insertJob([
        ['name', 'other'],
        ['next_run_at', new Date(now.getTime() - 9000)],
      ]);

      const first = await store.lockNext('w', lockOptions(now));
      const second = await store.lockNext('w', lockOptions(now));
      const third = await store.lockNext('w', lockOptions(now));

      expect(first?.data).toEqual({ n: 1 });
      expect(first?.lockedAt).toEqual(now);
      expect(second?.data).toEqual({ n: 2 });
      expect(third).toBeUndefined();
    });

    test('skips disabled jobs and jobs not due before the next scan', async () => {
      const now = new Date();
      await insertJob([
        ['name', 'w'],
        ['next_run_at', now],
        ['disabled', true],
      ]);
      await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() + MINUTE)],
      ]);
      expect(await store.lockNext('w', lockOptions(now))).toBeUndefined();
    });

    test('re-locks a job whose lock expired, but not a live lock', async () => {
      const now = new Date();
      await insertJob([
        ['name', 'live'],
        ['next_run_at', now],
        ['locked_at', new Date(now.getTime() - MINUTE)],
      ]);
      await insertJob([
        ['name', 'expired'],
        ['next_run_at', now],
        ['locked_at', new Date(now.getTime() - 11 * MINUTE)],
      ]);
      expect(await store.lockNext('live', lockOptions(now))).toBeUndefined();
      expect((await store.lockNext('expired', lockOptions(now)))?.lockedAt).toEqual(now);
    });

    test('concurrent lockers on separate connections never lock the same job', async () => {
      const due = new Date(Date.now() - 1000);
      for (let i = 0; i < 40; i++) {
        await insertJob([
          ['name', 'w'],
          ['next_run_at', due],
        ]);
      }
      const pools = Array.from({ length: 5 }, () => new Pool({ connectionString: testDatabaseUrl(), max: 2 }));
      const lockers = pools.map((pool) => new PgJobStore(pool, db.table));

      const drain = async (locker: PgJobStore): Promise<string[]> => {
        const locked: string[] = [];
        for (;;) {
          const record = await locker.lockNext('w', lockOptions());
          if (!record) {
            return locked;
          }
          locked.push(record._id);
        }
      };

      try {
        const results = await Promise.all(lockers.map(drain));
        const all = results.flat();
        expect(all).toHaveLength(40);
        expect(new Set(all).size).toBe(40);
      } finally {
        await Promise.all(pools.map((pool) => pool.end()));
      }
    });
  });

  describe('lockById', () => {
    test('locks only an unlocked, enabled job whose nextRunAt still matches', async () => {
      const now = new Date();
      const nextRunAt = new Date(now.getTime() - 1000);
      const { _id } = await insertJob([
        ['name', 'w'],
        ['next_run_at', nextRunAt],
      ]);

      expect(await store.lockById(_id, new Date(0), now)).toBeUndefined();
      expect((await store.lockById(_id, nextRunAt, now))?.lockedAt).toEqual(now);
      expect(await store.lockById(_id, nextRunAt, now)).toBeUndefined();
    });

    test('matches a job with no nextRunAt when null is passed', async () => {
      const now = new Date();
      const { _id } = await insertJob([['name', 'w']]);
      expect((await store.lockById(_id, null, now))?.lockedAt).toEqual(now);
    });
  });

  describe('resumeUnfinished', () => {
    const now = new Date();
    const defaultDeadline = new Date(now.getTime() - 10 * MINUTE);
    const stored = async (id: string) => (await store.find({ ids: [id] }))[0];

    test('releases an expired lock and reschedules the job now', async () => {
      const { _id } = await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() + MINUTE)],
        ['locked_at', new Date(now.getTime() - 11 * MINUTE)],
        ['last_run_at', new Date(now.getTime() - 11 * MINUTE)],
        ['last_modified_by', 'crashed-worker'],
      ]);

      expect(await store.resumeUnfinished(now, {}, defaultDeadline)).toBe(1);

      const record = await stored(_id);
      expect(record.lockedAt).toBeUndefined();
      expect(record.lastRunAt).toBeUndefined();
      expect(record.lastModifiedBy).toBeUndefined();
      expect(record.nextRunAt).toEqual(now);
    });

    test('leaves a live lock alone, even for a first run with no counters', async () => {
      const lockedAt = new Date(now.getTime() - MINUTE);
      const nextRunAt = new Date(now.getTime() + MINUTE);
      const { _id } = await insertJob([
        ['name', 'w'],
        ['next_run_at', nextRunAt],
        ['locked_at', lockedAt],
      ]);

      expect(await store.resumeUnfinished(now, {}, defaultDeadline)).toBe(0);
      expect((await stored(_id)).lockedAt).toEqual(lockedAt);
      expect((await stored(_id)).nextRunAt).toEqual(nextRunAt);
    });

    test("uses the job's own lock deadline when one is given", async () => {
      const { _id } = await insertJob([
        ['name', 'short'],
        ['next_run_at', new Date(now.getTime() + MINUTE)],
        ['locked_at', new Date(now.getTime() - 2 * MINUTE)],
      ]);
      const deadlines = { short: new Date(now.getTime() - MINUTE) };

      expect(await store.resumeUnfinished(now, deadlines, defaultDeadline)).toBe(1);
      expect((await stored(_id)).lockedAt).toBeUndefined();
    });

    test('honours a per-name lock lifetime longer than the default', async () => {
      const lockedAt = new Date(now.getTime() - 11 * MINUTE);
      const { _id } = await insertJob([
        ['name', 'long'],
        ['next_run_at', new Date(now.getTime() + MINUTE)],
        ['locked_at', lockedAt],
      ]);
      const deadlines = { long: new Date(now.getTime() - 60 * MINUTE) };

      expect(await store.resumeUnfinished(now, deadlines, defaultDeadline)).toBe(0);
      expect((await stored(_id)).lockedAt).toEqual(lockedAt);
    });

    test('does not release an expired lock on a job that finished its last run', async () => {
      const { _id } = await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() + MINUTE)],
        ['locked_at', new Date(now.getTime() - 11 * MINUTE)],
        ['run_count', 2],
        ['finished_count', 1],
        ['last_finished_at', new Date(now.getTime() - 20 * MINUTE)],
      ]);
      expect(await store.resumeUnfinished(now, {}, defaultDeadline)).toBe(0);
      expect((await stored(_id)).lockedAt).toBeDefined();
    });

    test('reschedules a never-run job that is past due, and leaves future or finished ones alone', async () => {
      const pastDue = await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() - MINUTE)],
      ]);
      const future = await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() + MINUTE)],
      ]);
      const finished = await insertJob([
        ['name', 'w'],
        ['next_run_at', new Date(now.getTime() - MINUTE)],
        ['last_finished_at', new Date(now.getTime() - MINUTE)],
      ]);

      expect(await store.resumeUnfinished(now, {}, defaultDeadline)).toBe(1);
      expect((await stored(pastDue._id)).nextRunAt).toEqual(now);
      expect((await stored(future._id)).nextRunAt).toEqual(future.nextRunAt);
      expect((await stored(finished._id)).nextRunAt).toEqual(finished.nextRunAt);
    });
  });
});
