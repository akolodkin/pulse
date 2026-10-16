import { Pool } from 'pg';
import { Job, Pulse } from '../../src';
import { PgJobStore } from '../../src/store';
import { createTestDb, TestDb, testDatabaseUrl } from '../helpers/pg.helper';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const waitFor = async (condition: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) {
      throw new Error(`condition not met within ${timeoutMs}ms`);
    }
    await delay(100);
  }
};

describe('several Pulse instances sharing one table', () => {
  let db: TestDb;
  let store: PgJobStore;
  let pools: Pool[];
  let instances: Pulse[];

  const newInstance = async (): Promise<Pulse> => {
    const pool = new Pool({ connectionString: testDatabaseUrl(), max: 5 });
    pools.push(pool);
    const pulse = new Pulse({ pg: { pool, table: db.table }, processEvery: '1 second' });
    await pulse._ready;
    instances.push(pulse);
    return pulse;
  };

  beforeAll(async () => {
    db = await createTestDb();
    store = new PgJobStore(db.pool, db.table);
    await store.ensureSchema();
  });

  beforeEach(async () => {
    await db.pool.query(`TRUNCATE ${db.table}`);
    pools = [];
    instances = [];
  });

  afterEach(async () => {
    await Promise.all(instances.map((pulse) => pulse.stop()));
    await Promise.all(pools.map((pool) => pool.end()));
  });

  afterAll(async () => {
    await db.cleanup();
  });

  test('every due job runs exactly once across three workers', async () => {
    const runs: string[] = [];
    const workers = await Promise.all([newInstance(), newInstance(), newInstance()]);
    workers.forEach((pulse) =>
      pulse.define('work', async (job: Job) => {
        runs.push(job.attrs._id);
        await delay(20);
      })
    );

    const scheduler = await newInstance();
    for (let i = 0; i < 30; i++) {
      await scheduler.create('work', { i }).save();
    }

    await Promise.all(workers.map((pulse) => pulse.start()));
    await waitFor(() => runs.length >= 30, 20_000);
    await delay(2500);

    expect(runs).toHaveLength(30);
    expect(new Set(runs).size).toBe(30);
    expect(await store.count({ name: 'work', lastFinishedAt: { isNull: true } })).toBe(0);
    expect(await store.count({ name: 'work', lockedAt: { isNull: false } })).toBe(0);
  });

  test('every() called by several booting instances leaves one row', async () => {
    const booting = await Promise.all([newInstance(), newInstance(), newInstance()]);
    await Promise.all(booting.map((pulse) => pulse.every('5 minutes', 'tick')));
    expect(await store.count({ name: 'tick' })).toBe(1);
  });

  test('a second instance starting up does not steal a job running on the first', async () => {
    let release: () => void = () => {};
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    let runs = 0;

    const first = await newInstance();
    first.define(
      'slow',
      async () => {
        runs++;
        await released;
      },
      { lockLifetime: 60_000 }
    );
    await first.start();
    await first.now('slow');
    await waitFor(() => runs === 1, 10_000);

    const second = await newInstance();
    second.define('slow', async () => {
      runs++;
    });
    await second.start();
    await delay(2500);

    expect(await store.count({ name: 'slow', lockedAt: { isNull: false } })).toBe(1);
    expect(runs).toBe(1);

    release();
    await waitFor(async () => (await store.count({ name: 'slow', lockedAt: { isNull: false } })) === 0, 10_000);
    expect(runs).toBe(1);
  });
});
