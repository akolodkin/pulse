/* eslint-disable no-console,no-unused-expressions,@typescript-eslint/no-unused-expressions */

import { Pulse } from '../../src';
import { Job } from '../../src/job';
import { JobSort, PgJobStore } from '../../src/store';
import { createTestDb, TestDb } from '../helpers/pg.helper';

describe('Test Pulse', () => {
  const jobTimeout = 500;
  const jobProcessor = () => {};

  let db: TestDb;
  let store: PgJobStore;
  let globalPulseInstance: Pulse;
  const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
  // Reads the stored row directly. Going through jobs() would build a Job,
  // whose constructor replaces a missing nextRunAt with "now".
  const storedJob = async (name: string) => (await store.find({ name }))[0];

  beforeAll(async () => {
    db = await createTestDb();
    store = new PgJobStore(db.pool, db.table);
    await store.ensureSchema();
  });

  afterAll(async () => {
    await db.cleanup();
  });

  beforeEach(async () => {
    await db.pool.query(`TRUNCATE ${db.table}`);
    globalPulseInstance = new Pulse({ pg: { pool: db.pool, table: db.table } });
    await globalPulseInstance._ready;
    ['someJob', 'send email', 'some job'].forEach((job) => {
      globalPulseInstance.define(job, jobProcessor);
    });
  });

  afterEach(async () => {
    await globalPulseInstance.stop();
  });

  describe('Test config', () => {
    describe('Test pg connection', () => {
      test('attaches a store when passed as an option', () => {
        const pulse = new Pulse({ pg: { pool: db.pool, table: db.table } });
        expect(pulse._store).toBeInstanceOf(PgJobStore);
      });

      test('attaches a store with pulse.pg() and returns itself', () => {
        const pulse = new Pulse();
        expect(pulse.pg(db.pool, db.table)).toBe(pulse);
        expect(pulse._store).toBeInstanceOf(PgJobStore);
      });

      test('emits ready after the constructor returns, so listeners attached right after it fire', async () => {
        const pulse = new Pulse({ pg: { pool: db.pool, table: db.table } });
        await expect(new Promise((resolve) => pulse.once('ready', () => resolve('ready')))).resolves.toBe('ready');
      });

      test('creates the table when autoSchema is true', async () => {
        const table = `${db.schema}.auto_jobs`;
        const pulse = new Pulse({ pg: { pool: db.pool, table }, autoSchema: true });
        await pulse._ready;
        const { rows } = await db.pool.query('SELECT to_regclass($1) AS t', [table]);
        expect(rows[0].t).not.toBeNull();
      });

      test('does not create the table by default', async () => {
        const table = `${db.schema}.manual_jobs`;
        const pulse = new Pulse({ pg: { pool: db.pool, table } });
        await pulse._ready;
        const { rows } = await db.pool.query('SELECT to_regclass($1) AS t', [table]);
        expect(rows[0].t).toBeNull();
      });

      test('emits error when autoSchema fails', async () => {
        const pulse = new Pulse({ pg: { pool: db.pool, table: 'no_such_schema.jobs' }, autoSchema: true });
        await expect(new Promise((_, reject) => pulse.once('error', reject))).rejects.toThrow(/no_such_schema/);
      });

      test('rejects an invalid table name', () => {
        expect(() => new Pulse({ pg: { pool: db.pool, table: 'jobs; DROP TABLE x' } })).toThrow(/Invalid table name/);
      });

      test('close() leaves the caller-owned pool open', async () => {
        expect(await globalPulseInstance.close()).toBe(globalPulseInstance);
        await expect(db.pool.query('SELECT 1')).resolves.toBeDefined();
      });
    });

    describe('Test processEvery', () => {
      test('sets the default processEvery', () => {
        expect(globalPulseInstance._processEvery).toEqual(5000);
      });
      test('sets the custom processEvery', () => {
        const pulse = new Pulse({ processEvery: '3 minutes' });
        expect(pulse._processEvery).toEqual(180000);
      });
      test('returns itself', () => {
        expect(globalPulseInstance.processEvery('3 minutes')).toEqual(globalPulseInstance);
      });
    });

    describe('Test name', () => {
      test('sets the pulse name', () => {
        globalPulseInstance.name('test queue');
        expect(globalPulseInstance._name).toEqual('test queue');
      });
      test('returns itself', () => {
        expect(globalPulseInstance.name('test queue')).toEqual(globalPulseInstance);
      });
    });

    describe('Test maxConcurrency', () => {
      test('sets the default maxConcurrency', () => {
        expect(globalPulseInstance._maxConcurrency).toEqual(20);
      });

      test('sets the custom maxConcurrency', () => {
        globalPulseInstance.maxConcurrency(10);
        expect(globalPulseInstance._maxConcurrency).toEqual(10);
      });
      test('returns itself', () => {
        expect(globalPulseInstance.maxConcurrency(10)).toEqual(globalPulseInstance);
      });
    });

    describe('Test default defaultConcurrency', () => {
      test('sets the defaultConcurrency', () => {
        expect(globalPulseInstance._defaultConcurrency).toEqual(5);
      });

      test('sets the custom defaultConcurrency', () => {
        globalPulseInstance.defaultConcurrency(1);
        expect(globalPulseInstance._defaultConcurrency).toEqual(1);
      });
      test('returns itself', () => {
        expect(globalPulseInstance.defaultConcurrency(1)).toEqual(globalPulseInstance);
      });

      test('is inherited by jobs', () => {
        globalPulseInstance.defaultConcurrency(10);
        globalPulseInstance.define('testDefaultConcurrency', () => {});
        expect(globalPulseInstance._definitions.testDefaultConcurrency.concurrency).toEqual(10);
      });
    });

    describe('Test lockLimit', () => {
      test('sets the default lockLimit', () => {
        expect(globalPulseInstance._lockLimit).toEqual(0);
      });

      test('sets the custom lockLimit', () => {
        globalPulseInstance.lockLimit(10);
        expect(globalPulseInstance._lockLimit).toEqual(10);
      });
      test('returns itself', () => {
        expect(globalPulseInstance.lockLimit(10)).toEqual(globalPulseInstance);
      });
    });

    describe('Test defaultLockLimit', () => {
      test('sets the default defaultLockLimit', () => {
        expect(globalPulseInstance._defaultLockLimit).toEqual(0);
      });

      test('sets the custom defaultLockLimit', () => {
        globalPulseInstance.defaultLockLimit(1);
        expect(globalPulseInstance._defaultLockLimit).toEqual(1);
      });
      test('returns itself', () => {
        expect(globalPulseInstance.defaultLockLimit(5)).toEqual(globalPulseInstance);
      });

      test('is inherited by jobs', () => {
        globalPulseInstance.defaultLockLimit(10);
        globalPulseInstance.define('testDefaultLockLimit', () => {});
        expect(globalPulseInstance._definitions.testDefaultLockLimit.lockLimit).toEqual(10);
      });
    });

    describe('Test defaultLockLifetime', () => {
      test('sets the default defaultLockLifetime', () => {
        expect(globalPulseInstance._defaultLockLifetime).toEqual(600000);
      });

      test('sets the custom defaultLockLifetime', () => {
        globalPulseInstance.defaultLockLifetime(9999);
        expect(globalPulseInstance._defaultLockLifetime).toEqual(9999);
      });

      test('returns itself', () => {
        expect(globalPulseInstance.defaultLockLifetime(1000)).toEqual(globalPulseInstance);
      });

      test('is inherited by jobs', () => {
        globalPulseInstance.defaultLockLifetime(7777);
        globalPulseInstance.define('testDefaultLockLifetime', () => {});
        expect(globalPulseInstance._definitions.testDefaultLockLifetime.lockLifetime).toEqual(7777);
      });
    });

    describe('Test sort', () => {
      test('sets the default sort', () => {
        expect(globalPulseInstance._sort).toEqual({ nextRunAt: 'asc', priority: 'desc' });
      });

      test('sets the custom sort', () => {
        globalPulseInstance.sort({ nextRunAt: 'desc' });
        expect(globalPulseInstance._sort).toEqual({ nextRunAt: 'desc' });
      });

      test('rejects a Mongo-style sort in the config', () => {
        expect(() => new Pulse({ sort: { nextRunAt: 1 } as unknown as JobSort })).toThrow(/Invalid sort direction/);
      });

      test('rejects an unknown sort field in sort()', () => {
        expect(() => globalPulseInstance.sort({ bogus: 'asc' } as unknown as JobSort)).toThrow(/Cannot sort jobs by/);
      });

      test('returns itself', () => {
        expect(globalPulseInstance.sort({ nextRunAt: 'asc', priority: 'asc' })).toEqual(globalPulseInstance);
      });
    });

    describe('Test resumeOnRestart', () => {
      const MINUTE = 60_000;

      test('sets the default resumeOnRestart', () => {
        expect(globalPulseInstance._resumeOnRestart).toBeTruthy();
      });

      test('sets the custom resumeOnRestart', () => {
        globalPulseInstance.resumeOnRestart(false);
        expect(globalPulseInstance._resumeOnRestart).toBeFalsy();
      });

      test('returns itself', () => {
        expect(globalPulseInstance.resumeOnRestart(false)).toEqual(globalPulseInstance);
      });

      test('reschedules a never-run job that is past due', async () => {
        const job = globalPulseInstance.create('sendEmail', { to: 'user@example.com' });
        job.attrs.nextRunAt = new Date(Date.now() - 2000);
        await job.save();

        const before = Date.now();
        await globalPulseInstance._resumeUnfinishedJobs();

        expect((await storedJob('sendEmail')).nextRunAt!.getTime()).toBeGreaterThanOrEqual(before);
      });

      test('should not modify jobs with existing nextRunAt', async () => {
        const futureDate = new Date(Date.now() + 60 * 60 * 1000);
        const job = globalPulseInstance.create('sendEmail', { to: 'user@example.com' });
        job.attrs.nextRunAt = futureDate;
        await job.save();

        await globalPulseInstance._resumeUnfinishedJobs();

        expect((await storedJob('sendEmail')).nextRunAt).toEqual(futureDate);
      });

      test('releases an expired lock and reschedules the job now', async () => {
        const job = globalPulseInstance.create('processData', { data: 'sample' });
        job.attrs.repeatInterval = '5 minutes';
        job.attrs.nextRunAt = new Date(Date.now() + MINUTE);
        job.attrs.lockedAt = new Date(Date.now() - globalPulseInstance._defaultLockLifetime - MINUTE);
        await job.save();
        await store.update(job.attrs._id, [['last_modified_by', 'server_crash']]);

        const before = Date.now();
        await globalPulseInstance._resumeUnfinishedJobs();

        const stored = await storedJob('processData');
        expect(stored.lockedAt).toBeUndefined();
        expect(stored.lastModifiedBy).toBeUndefined();
        expect(stored.nextRunAt!.getTime()).toBeGreaterThanOrEqual(before);
      });

      test('keeps a live lock held by another instance', async () => {
        const lockedAt = new Date(Date.now() - 1000);
        const nextRunAt = new Date(Date.now() + MINUTE);
        const job = globalPulseInstance.create('processData', { data: 'sample' });
        job.attrs.repeatInterval = '10 minutes';
        job.attrs.lockedAt = lockedAt;
        job.attrs.nextRunAt = nextRunAt;
        await job.save();

        await globalPulseInstance._resumeUnfinishedJobs();

        const stored = await storedJob('processData');
        expect(stored.lockedAt).toEqual(lockedAt);
        expect(stored.nextRunAt).toEqual(nextRunAt);
      });

      test("uses the job definition's lockLifetime", async () => {
        globalPulseInstance.define('shortLock', jobProcessor, { lockLifetime: 1000 });
        const job = globalPulseInstance.create('shortLock', {});
        job.attrs.nextRunAt = new Date(Date.now() + MINUTE);
        job.attrs.lockedAt = new Date(Date.now() - 5000);
        await job.save();

        await globalPulseInstance._resumeUnfinishedJobs();

        expect((await storedJob('shortLock')).lockedAt).toBeUndefined();
      });

      test('start() resumes unfinished jobs when resumeOnRestart is on', async () => {
        const job = globalPulseInstance.create('sendEmail', {});
        job.attrs.nextRunAt = new Date(Date.now() + MINUTE);
        job.attrs.lockedAt = new Date(Date.now() - 2 * globalPulseInstance._defaultLockLifetime);
        await job.save();

        await globalPulseInstance.start();

        expect((await storedJob('sendEmail')).lockedAt).toBeUndefined();
      });

      test('start() leaves locks alone when resumeOnRestart is off', async () => {
        const job = globalPulseInstance.create('sendEmail', {});
        job.attrs.nextRunAt = new Date(Date.now() + MINUTE);
        job.attrs.lockedAt = new Date(Date.now() - 2 * globalPulseInstance._defaultLockLifetime);
        await job.save();

        globalPulseInstance.resumeOnRestart(false);
        await globalPulseInstance.start();

        expect((await storedJob('sendEmail')).lockedAt).toBeDefined();
      });

      test('should compute nextRunAt after running a recurring job', async () => {
        let executionCount = 0;

        globalPulseInstance.define('recurringJob', async () => {
          executionCount++;
        });

        const job = globalPulseInstance.create('recurringJob', { key: 'value' });
        job.attrs.repeatInterval = '5 minutes';
        await job.save();

        globalPulseInstance.processEvery('1 second');
        await globalPulseInstance.start();

        await new Promise((resolve) => setTimeout(resolve, 4000));

        const stored = await storedJob('recurringJob');

        expect(executionCount).toBeGreaterThan(0);
        expect(stored.lastRunAt).toBeDefined();
        expect(stored.lockedAt).toBeUndefined();
        expect(stored.nextRunAt!.getTime()).toBeGreaterThan(Date.now());
      });
    });
  });
  describe('Test job methods', () => {
    describe('Test create method', () => {
      let job: any;
      beforeEach(() => {
        job = globalPulseInstance.create('sendEmail', { to: 'some guy' });
      });

      test('returns a job', () => {
        expect(job).toBeInstanceOf(Job);
      });
      test('sets the name', () => {
        expect(job.attrs.name).toEqual('sendEmail');
      });
      test('sets the type', () => {
        expect(job.attrs.type).toEqual('normal');
      });
      test('sets the pulse', () => {
        expect(job.pulse).toEqual(globalPulseInstance);
      });
      test('sets the data', () => {
        expect(job.attrs.data.to).toBe('some guy');
      });
    });

    describe('Test define method', () => {
      test('stores the definition for the job', () => {
        expect(globalPulseInstance._definitions.someJob.fn).toBe(jobProcessor);
      });
      describe('Test default options', () => {
        test('sets the default concurrency for the job', () => {
          expect(globalPulseInstance._definitions.someJob.concurrency).toBe(5);
        });
        test('sets the default lockLimit for the job', () => {
          expect(globalPulseInstance._definitions.someJob.lockLimit).toBe(0);
        });

        test('sets the default lockLifetime for the job', () => {
          expect(globalPulseInstance._definitions.someJob.lockLifetime).toBe(600000);
        });

        test('sets the default priority for the job', () => {
          expect(globalPulseInstance._definitions.someJob.priority).toBe(0);
        });

        test('sets the default attempts for the job', () => {
          expect(globalPulseInstance._definitions.someJob.attempts).toBe(0);
        });
      });

      describe('Test setting options', () => {
        test('sets the priority option for the job', () => {
          globalPulseInstance.define('highPriority', jobProcessor, { priority: 'high' });
          expect(globalPulseInstance._definitions.highPriority.priority).toBe('high');
        });

        test('sets shouldSaveResult option for the job', () => {
          globalPulseInstance.define('shouldSaveResultTrue', jobProcessor, { shouldSaveResult: true });
          expect(globalPulseInstance._definitions.shouldSaveResultTrue.shouldSaveResult).toBeTruthy();
        });

        test('sets attempts and backoff option for the job', () => {
          globalPulseInstance.define('attemptsAndBackoff', jobProcessor, {
            attempts: 5,
            backoff: { type: 'fixed', delay: 1000 },
          });
          expect(globalPulseInstance._definitions.attemptsAndBackoff.attempts).toBe(5);
          expect(globalPulseInstance._definitions.attemptsAndBackoff.backoff).toEqual({ type: 'fixed', delay: 1000 });
        });
      });
    });

    describe('Test every method', () => {
      describe('Test with a job name specified', () => {
        test('returns a job', async () => {
          expect(await globalPulseInstance.every('5 minutes', 'send email')).toBeInstanceOf(Job);
        });
        test('sets the repeatEvery', async () => {
          const result = (await globalPulseInstance.every('5 seconds', 'send email')) as Job;
          expect(result.attrs.repeatInterval).toEqual('5 seconds');
        });
        test('sets the pulse', async () => {
          const result = (await globalPulseInstance.every('5 seconds', 'send email')) as Job;
          expect(result.pulse).toEqual(globalPulseInstance);
        });
        test('should update a job that was previously scheduled with `every`', async () => {
          await globalPulseInstance.every('10', 'shouldBeSingleJob');
          await globalPulseInstance.every('20', 'shouldBeSingleJob');

          // Give the saves a little time to propagate
          await delay(jobTimeout);

          const res = await globalPulseInstance.jobs({ name: 'shouldBeSingleJob' });
          expect(res.length).toBe(1);
        });
        test('should not run immediately if options.skipImmediate is true', async () => {
          const jobName = 'send email';
          await globalPulseInstance.every('5 minutes', jobName, {}, { skipImmediate: true });
          const job = (await globalPulseInstance.jobs({ name: jobName }))[0] as Job;
          const nextRunAt = job.attrs.nextRunAt?.getTime() as number;
          const now = new Date().getTime();
          expect(nextRunAt - now > 0).toBe(true);
        });
        test('should run immediately if options.skipImmediate is false', async () => {
          const jobName = 'send email';
          await globalPulseInstance.every('5 minutes', jobName, {}, { skipImmediate: false });
          const job = (await globalPulseInstance.jobs({ name: jobName }))[0];
          const nextRunAt = job.attrs.nextRunAt?.getTime() as number;
          const now = new Date().getTime();
          expect(nextRunAt - now <= 0).toBe(true);
        });

        test('should update nextRunAt after running a recurring job', async () => {
          const job = globalPulseInstance.create('recurringJob', { data: 'test' });
          job.attrs.repeatInterval = '*/5 * * * *';
          await job.save();

          await job.run();

          expect(job.attrs.nextRunAt).not.toBeNull();
          expect(job.attrs.nextRunAt?.getTime()).toBeGreaterThan(Date.now());
        });
      });
      describe('Test with array of names specified', () => {
        test('returns array of jobs', async () => {
          const jobs = await globalPulseInstance.every('5 minutes', ['send email', 'some job']);
          expect(Array.isArray(jobs)).toBe(true);
        });
      });
    });

    describe('Test countJobs', () => {
      test('returns zero when there are no jobs', async () => {
        expect(await globalPulseInstance.countJobs()).toBe(0);
      });

      test('counts jobs correctly', async () => {
        await globalPulseInstance.create('testJob1', {}).save();
        await globalPulseInstance.create('testJob2', {}).save();

        expect(await globalPulseInstance.countJobs()).toBe(2);
      });

      test('counts jobs matching a filter', async () => {
        await globalPulseInstance.create('testJob1', { type: 'email' }).save();
        await globalPulseInstance.create('testJob2', { type: 'sms' }).save();

        expect(await globalPulseInstance.countJobs({ data: { type: 'email' } })).toBe(1);
      });
    });
    describe('Test schedule method', () => {
      test('creates and schedules a job', async () => {
        await globalPulseInstance.schedule('2024-06-03T10:00:00Z', 'sendEmail', { to: 'some guy' });
        const jobs = await globalPulseInstance.jobs({ name: 'sendEmail' });
        expect(jobs.length).toBe(1);
      });

      test('creates and schedules multiple jobs', async () => {
        await globalPulseInstance.schedule('2024-06-03T10:00:00Z', ['sendEmail', 'some job'], { to: 'some guy' });
        const jobs = await globalPulseInstance.jobs();
        expect(jobs.length).toBe(2);
      });

      test('checks if job is scheduled correctly', async () => {
        await globalPulseInstance.schedule('2024-06-03T10:00:00Z', 'sendEmail', { to: 'some guy' });
        const jobs = await globalPulseInstance.jobs({ name: 'sendEmail' });
        const job = jobs[0];
        expect(job.attrs.nextRunAt).toEqual(new Date('2024-06-03T10:00:00Z'));
      });
    });
  });

  describe('Test query and persistence API', () => {
    test('jobs() applies sort, limit and skip', async () => {
      const base = Date.now() + 60_000;
      for (const [name, offset] of [
        ['a', 1],
        ['b', 2],
        ['c', 3],
      ] as const) {
        await globalPulseInstance
          .create(name, {})
          .schedule(new Date(base + offset * 1000))
          .save();
      }

      const page = await globalPulseInstance.jobs({}, { sort: { nextRunAt: 'desc' }, limit: 2, skip: 1 });

      expect(page.map((job) => job.attrs.name)).toEqual(['b', 'a']);
    });

    test('jobs() returns Job instances with string ids', async () => {
      await globalPulseInstance.create('sendEmail', {}).save();
      const [job] = await globalPulseInstance.jobs({ name: 'sendEmail' });
      expect(job).toBeInstanceOf(Job);
      expect(job.attrs._id).toMatch(/^[0-9a-f-]{36}$/);
    });

    test('jobs() rejects a leftover Mongo-style filter', async () => {
      await expect(
        globalPulseInstance.jobs({ failedAt: { $exists: true } } as unknown as Parameters<Pulse['jobs']>[0])
      ).rejects.toThrow(/Unknown date condition/);
    });

    test('cancel() deletes matching jobs, emits cancel and returns the count', async () => {
      const job = await globalPulseInstance.create('sendEmail', {}).save();
      await globalPulseInstance.create('sendEmail', {}).save();
      const cancelled = jest.fn();
      globalPulseInstance.on('cancel', cancelled);

      expect(await globalPulseInstance.cancel({ ids: [job.attrs._id] })).toBe(1);
      expect(cancelled).toHaveBeenCalledWith(1);
      expect(await globalPulseInstance.countJobs({ name: 'sendEmail' })).toBe(1);
    });

    test('cancel() refuses an undefined filter value and keeps the job', async () => {
      await globalPulseInstance.create('sendEmail', {}).save();
      await expect(globalPulseInstance.cancel({ ids: undefined })).rejects.toThrow(/is undefined/);
      expect(await globalPulseInstance.countJobs({ name: 'sendEmail' })).toBe(1);
    });

    test('job.remove() deletes the job', async () => {
      const job = await globalPulseInstance.create('sendEmail', {}).save();
      expect(await job.remove()).toBe(1);
      expect(await globalPulseInstance.countJobs()).toBe(0);
    });

    test('saving a job that was cancelled meanwhile is a no-op', async () => {
      const job = await globalPulseInstance.create('sendEmail', {}).save();
      await globalPulseInstance.cancel({ ids: [job.attrs._id] });
      job.attrs.failReason = 'late write';
      await expect(job.save()).resolves.toBe(job);
      expect(await globalPulseInstance.countJobs()).toBe(0);
    });

    test('disable() and enable() return the number of jobs changed', async () => {
      await globalPulseInstance.create('a', {}).save();
      await globalPulseInstance.create('b', {}).save();

      expect(await globalPulseInstance.disable({ name: 'a' })).toBe(1);
      expect(await globalPulseInstance.disable({ name: 'a' })).toBe(0);
      expect((await storedJob('a')).disabled).toBe(true);
      expect(await globalPulseInstance.enable()).toBe(1);
      expect((await storedJob('a')).disabled).toBe(false);
    });

    test('purge() removes only jobs with no definition', async () => {
      await globalPulseInstance.create('someJob', {}).save();
      await globalPulseInstance.create('undefinedJob', {}).save();

      expect(await globalPulseInstance.purge()).toBe(1);
      expect((await globalPulseInstance.jobs()).map((job) => job.attrs.name)).toEqual(['someJob']);
    });

    test('unique() upserts on the key; insertOnly keeps the first data', async () => {
      const first = await globalPulseInstance.create('u', { v: 1 }).unique({ patientId: 'p1' }).save();
      const second = await globalPulseInstance.create('u', { v: 2 }).unique({ patientId: 'p1' }).save();
      const third = await globalPulseInstance
        .create('u', { v: 3 })
        .unique({ patientId: 'p1' }, { insertOnly: true })
        .save();

      expect(second.attrs._id).toBe(first.attrs._id);
      expect(third.attrs._id).toBe(first.attrs._id);
      expect(await globalPulseInstance.countJobs({ name: 'u' })).toBe(1);
      expect((await storedJob('u')).data).toEqual({ v: 2 });
    });

    test('unique() keys compare regardless of key order', async () => {
      await globalPulseInstance.create('u', {}).unique({ a: 1, b: 2 }).save();
      await globalPulseInstance.create('u', {}).unique({ b: 2, a: 1 }).save();
      expect(await globalPulseInstance.countJobs({ name: 'u' })).toBe(1);
    });

    test('every() called again does not clear a lock or counters it does not carry', async () => {
      const job = (await globalPulseInstance.every('5 minutes', 'tick')) as Job;
      const lockedAt = new Date();
      await store.update(job.attrs._id, [
        ['locked_at', lockedAt],
        ['run_count', 4],
      ]);

      await globalPulseInstance.every('5 minutes', 'tick');

      const stored = await storedJob('tick');
      expect(stored.lockedAt).toEqual(lockedAt);
      expect(stored.runCount).toBe(4);
    });

    test('setting an attribute to undefined and saving clears it (admin retry)', async () => {
      const job = await globalPulseInstance.create('sendEmail', {}).save();
      await store.update(job.attrs._id, [
        ['locked_at', new Date()],
        ['failed_at', new Date()],
        ['fail_reason', 'boom'],
      ]);
      const [loaded] = await globalPulseInstance.jobs({ ids: [job.attrs._id] });

      loaded.attrs.lockedAt = undefined;
      loaded.attrs.failedAt = undefined;
      loaded.attrs.failReason = undefined;
      await loaded.save();

      const stored = await storedJob('sendEmail');
      expect(stored.lockedAt).toBeUndefined();
      expect(stored.failedAt).toBeUndefined();
      expect(stored.failReason).toBeUndefined();
    });

    test('fetchStatus() reads the stored job, and reports a removed one', async () => {
      const job = await globalPulseInstance.create('sendEmail', {}).save();
      expect((await job.fetchStatus()).status).toBe(true);
      await job.remove();
      expect(await job.fetchStatus()).toEqual({
        status: false,
        reason: `job with id ${job.attrs._id} not found in database`,
      });
    });

    test('Dates inside data come back as ISO strings', async () => {
      const when = new Date('2026-01-01T00:00:00.000Z');
      await globalPulseInstance.create('sendEmail', { when }).save();
      expect((await storedJob('sendEmail')).data).toEqual({ when: '2026-01-01T00:00:00.000Z' });
    });

    test('now() runs a defined job and leaves it finished and unlocked', async () => {
      const ran = jest.fn();
      globalPulseInstance.define('quick', async () => ran());
      globalPulseInstance.processEvery('1 minute');
      await globalPulseInstance.start();

      await globalPulseInstance.now('quick');
      await delay(2500);

      expect(ran).toHaveBeenCalledTimes(1);
      const stored = await storedJob('quick');
      expect(stored.lastFinishedAt).toBeDefined();
      expect(stored.lockedAt).toBeUndefined();
    });

    test('job.run() rejects when the save before running fails, instead of never settling', async () => {
      globalPulseInstance.define('flaky-db', async () => {});
      const job = await globalPulseInstance.create('flaky-db', {}).save();
      jest.spyOn(globalPulseInstance._store, 'update').mockRejectedValueOnce(new Error('db down'));

      await expect(job.run()).rejects.toThrow('db down');
    }, 3000);

    test('stop() keeps the lock of a running job and releases jobs that were locked but not started', async () => {
      let release: () => void = () => {};
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      const started: string[] = [];
      globalPulseInstance.define(
        'single-lane',
        async (job: Job) => {
          started.push(job.attrs._id);
          await released;
        },
        { concurrency: 1 }
      );
      const running = await globalPulseInstance.create('single-lane', { n: 1 }).save();
      const waiting = await globalPulseInstance.create('single-lane', { n: 2 }).save();
      globalPulseInstance.processEvery('1 minute');
      await globalPulseInstance.start();
      await delay(1000);
      expect(started).toHaveLength(1);
      const [runningId] = started;
      const waitingId = runningId === running.attrs._id ? waiting.attrs._id : running.attrs._id;
      expect((await store.find({ ids: [waitingId] }))[0].lockedAt).toBeDefined();

      await globalPulseInstance.stop();

      expect((await store.find({ ids: [runningId] }))[0].lockedAt).toBeDefined();
      expect((await store.find({ ids: [waitingId] }))[0].lockedAt).toBeUndefined();

      release();
      await delay(500);
      const finished = (await store.find({ ids: [runningId] }))[0];
      expect(finished.lockedAt).toBeUndefined();
      expect(finished.lastFinishedAt).toBeDefined();
    });
  });
});
