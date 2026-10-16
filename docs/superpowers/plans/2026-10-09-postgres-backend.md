# Pulse on Postgres Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fork's MongoDB storage with PostgreSQL so LISA can drop MongoDB for its job queue, keeping every public Pulse behaviour.

**Architecture:** All SQL lives in `src/store/` behind one class, `PgJobStore`, plus a single row ⇄ `JobAttributes` mapper and a typed filter compiler. `Pulse` and `Job` methods call `this._store.*` instead of `this._collection.*`; the processing loop keeps its structure. Locking uses `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED LIMIT 1)` so several worker processes can poll one table.

**Tech Stack:** TypeScript 5, node-postgres (`pg` 8, peer dependency), Jest 29 + ts-jest, `embedded-postgres` (real Postgres 16 for tests, started from an ESM Jest `globalSetup`).

**Spec:** `docs/superpowers/specs/2026-10-09-postgres-backend-design.md`

## Global Constraints

- Postgres only: after Task 9 nothing in `src/` or `test/` imports `mongodb` or `mongodb-memory-server`.
- Pulse never creates, ends or owns the `pg.Pool`; callers pass it in.
- Default table name: `pulse_jobs`. Table names must match `table` or `schema.table` of `[A-Za-z_][A-Za-z0-9_]*` parts and are always double-quoted in SQL.
- Every store operation is a single SQL statement on the pool (autocommit); no explicit transactions, no held clients.
- Caller input never reaches SQL text: values are `$n` parameters; sort keys map through a whitelist.
- `attrs._id` is a uuid `string`.
- Only attributes present on the job are written (`Object.prototype.hasOwnProperty`); present-but-`undefined` → `NULL`; `NOT NULL DEFAULT` columns (`priority`, `disabled`, `should_save_result`) treat `undefined` as absent.
- `NULL` columns read back as absent (`undefined`) attributes.
- Timestamps used in lock predicates are generated in JS (`new Date()`) and passed as parameters.
- Dependencies: `pg ^8` peerDependency (and devDependency for tests), `@types/pg ^8` and `embedded-postgres 16.14.0-beta.17` devDependencies. No other new dependencies.
- Version `2.0.0`.
- Commits: Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`), no `Co-Authored-By` line.
- Build check: `npm run build`; lint: `npm run lint`; tests: `npm test`.

## Review Focus

1. **Leftover Mongo-shaped filters** (`{ _id: x }`, `{ nextRunAt: { $lte: d } }`, `{ 'data.type': 'x' }`) passed to `cancel()`/`jobs()` must throw, not match every row — otherwise a missed call site in LISA deletes the whole queue. Pinned in Task 4.
2. **Malformed ids from URLs** (`ids: ['not-a-uuid']`) must read as "not found", not a Postgres cast error (500). Pinned in Task 4.
3. **Re-saving must not clobber state it doesn't carry**: `every()` re-run by each booting replica, or a job loaded and saved, must leave `locked_at` and run counters alone. Pinned in Task 5 (store) and Task 7 (Pulse).
4. **Concurrent pollers** in separate processes must lock each due job exactly once. Pinned in Task 6 (store) and Task 8 (Pulse instances).
5. **`nameContains` with `%`, `_` or `\`** (admin search box) must match literally, not as LIKE wildcards. Pinned in Task 4.

---

## File Structure

| Path | Responsibility |
|---|---|
| `src/store/table.ts` (new) | Validate and quote the table name |
| `src/store/ddl.ts` (new) | `pulseJobsDdl(table)` — canonical `CREATE TABLE` / `CREATE INDEX` |
| `src/store/row-mapper.ts` (new) | Column list; `toColumnValues(attrs)`; `fromRow(row)`; `JobRecord` type |
| `src/store/canonical-json.ts` (new) | Key-sorted JSON for `unique()` keys |
| `src/store/filter.ts` (new) | `JobFilter`, `JobSort`, `DateCond`; `compileWhere`, `compileOrderBy`, `escapeLike` |
| `src/store/pg-job-store.ts` (new) | `PgJobStore`: every SQL statement |
| `src/store/index.ts` (new) | Re-exports for the store |
| `src/pulse/pg.ts` (new) | `pulse.pg(pool, table?)` — attach store, run `autoSchema`, emit `ready` |
| `src/pulse/ensure-schema.ts` (new) | `pulse.ensureSchema()` |
| `src/pulse/resume-unfinished-jobs.ts` (new) | Release expired locks on `start()` |
| `src/pulse/index.ts` | Config, fields, getters |
| `src/pulse/{save-job,jobs,count-jobs,cancel,disable,enable,purge,sort,close,stop,start,resume-on-restart,find-and-lock-next-job}.ts` | Call the store |
| `src/pulse/{database,db-init,mongo,has-mongo-protocol}.ts` | Deleted |
| `src/job/{index,unique,remove,fetch-status}.ts` | String ids, key-object `unique()` |
| `src/utils/{create-job,process-jobs}.ts` | Build jobs from `JobRecord`; lock-on-the-fly via store |
| `test/setup/global-setup.mjs`, `global-teardown.mjs` (new) | Start/stop embedded Postgres once per run |
| `test/helpers/pg.helper.ts` (new) | Isolated schema per test file |
| `test/unit/store/*.spec.ts` (new) | Store unit tests against Postgres |
| `test/unit/pulse.spec.ts` | Ported suite |
| `test/unit/multi-instance.spec.ts` (new) | Several Pulse instances on one table |
| `test/helpers/mock.helper.ts` | Deleted |

---

### Task 1: Embedded Postgres test infrastructure

**Files:**
- Modify: `package.json` (via `npm install`), `jest.config.ts`
- Create: `test/setup/global-setup.mjs`, `test/setup/global-teardown.mjs`, `test/helpers/pg.helper.ts`
- Test: `test/unit/pg-helper.spec.ts`

**Interfaces:**
- Produces: env var `PULSE_TEST_DATABASE_URL` (set by globalSetup); `createTestDb(): Promise<TestDb>` with `TestDb = { pool: Pool; schema: string; table: string; cleanup(): Promise<void> }` where `table` is `` `${schema}.pulse_jobs` `` (not yet created).

- [ ] **Step 1: Install the approved dependencies**

```bash
cd ~/Developer/work/li/pulse
npm install --save-dev --no-audit --no-fund pg@^8 @types/pg@^8 embedded-postgres@16.14.0-beta.17
```

Then add a `peerDependencies` block to `package.json` (after `dependencies`):

```json
  "peerDependencies": {
    "pg": "^8"
  },
```

- [ ] **Step 2: Write the failing helper test**

`test/unit/pg-helper.spec.ts`:

```ts
import { createTestDb } from '../helpers/pg.helper';

describe('createTestDb', () => {
  test('gives each caller its own schema and drops it on cleanup', async () => {
    const db = await createTestDb();
    const other = await createTestDb();

    expect(db.schema).not.toEqual(other.schema);
    expect(db.table).toEqual(`${db.schema}.pulse_jobs`);

    const exists = async (schema: string) =>
      (await other.pool.query('SELECT 1 FROM pg_namespace WHERE nspname = $1', [schema])).rowCount;

    expect(await exists(db.schema)).toBe(1);
    await db.cleanup();
    expect(await exists(db.schema)).toBe(0);
    await other.cleanup();
  });
});
```

- [ ] **Step 3: Run it to verify it fails**

Run: `npx jest test/unit/pg-helper.spec.ts`
Expected: FAIL — `Cannot find module '../helpers/pg.helper'`.

- [ ] **Step 4: Add the global setup/teardown and the helper**

`test/setup/global-setup.mjs` (ESM because `embedded-postgres` is ESM-only; Jest loads `.mjs` global setup natively):

```js
import EmbeddedPostgres from 'embedded-postgres';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export default async function globalSetup() {
  const port = 54000 + Math.floor(Math.random() * 1000);
  const pg = new EmbeddedPostgres({
    databaseDir: mkdtempSync(join(tmpdir(), 'pulse-pg-')),
    user: 'postgres',
    password: 'postgres',
    port,
    persistent: false,
    onLog: () => {},
  });
  await pg.initialise();
  await pg.start();
  await pg.createDatabase('pulse_test');
  globalThis.__PULSE_PG__ = pg;
  process.env.PULSE_TEST_DATABASE_URL = `postgresql://postgres:postgres@127.0.0.1:${port}/pulse_test`;
}
```

`test/setup/global-teardown.mjs`:

```js
export default async function globalTeardown() {
  await globalThis.__PULSE_PG__?.stop();
}
```

`test/helpers/pg.helper.ts`:

```ts
import { randomBytes } from 'crypto';
import { Pool } from 'pg';

export interface TestDb {
  pool: Pool;
  schema: string;
  table: string;
  cleanup: () => Promise<void>;
}

export const testDatabaseUrl = (): string => {
  const url = process.env.PULSE_TEST_DATABASE_URL;
  if (!url) {
    throw new Error('PULSE_TEST_DATABASE_URL is not set; run the tests through jest so globalSetup starts Postgres');
  }
  return url;
};

export async function createTestDb(): Promise<TestDb> {
  const pool = new Pool({ connectionString: testDatabaseUrl(), max: 10 });
  const schema = `t_${randomBytes(6).toString('hex')}`;
  await pool.query(`CREATE SCHEMA ${schema}`);

  return {
    pool,
    schema,
    table: `${schema}.pulse_jobs`,
    cleanup: async () => {
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
      await pool.end();
    },
  };
}
```

`jest.config.ts`:

```ts
import type { Config } from '@jest/types';
// Sync object
const config: Config.InitialOptions = {
  preset: 'ts-jest',
  reporters: ['default', 'jest-junit'],
  globalSetup: './test/setup/global-setup.mjs',
  globalTeardown: './test/setup/global-teardown.mjs',
  testTimeout: 30000,
};
export default config;
```

- [ ] **Step 5: Run the helper test and the existing suite**

Run: `npx jest test/unit/pg-helper.spec.ts`
Expected: PASS (1 test).

Run: `npm test`
Expected: PASS — 74 tests (the 73 Mongo tests still run on `mongodb-memory-server` until Task 7).

- [ ] **Step 6: Commit**

```bash
git add package.json package-lock.json jest.config.ts test/setup test/helpers/pg.helper.ts test/unit/pg-helper.spec.ts
git commit -m "test: start an embedded Postgres for the test run"
```

---

### Task 2: Table name and DDL

**Files:**
- Create: `src/store/table.ts`, `src/store/ddl.ts`
- Test: `test/unit/store/ddl.spec.ts`

**Interfaces:**
- Consumes: `createTestDb()` (Task 1); `PulseError` from `src/utils/error.ts`.
- Produces: `quoteTableName(table: string): string` (throws `PulseError` matching `/Invalid table name/`); `pulseJobsDdl(table: string): string`.

- [ ] **Step 1: Write the failing tests**

`test/unit/store/ddl.spec.ts`:

```ts
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
```

- [ ] **Step 2: Run to verify failure**

Run: `npx jest test/unit/store/ddl.spec.ts`
Expected: FAIL — `Cannot find module '../../../src/store/ddl'`.

- [ ] **Step 3: Implement**

`src/store/table.ts`:

```ts
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
```

`src/store/ddl.ts`:

```ts
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
```

- [ ] **Step 4: Run to verify pass**

Run: `npx jest test/unit/store/ddl.spec.ts`
Expected: PASS (14 tests).

- [ ] **Step 5: Commit**

```bash
git add src/store/table.ts src/store/ddl.ts test/unit/store/ddl.spec.ts
git commit -m "feat(store): add the Postgres jobs table DDL"
```

---

### Task 3: Row mapper and canonical JSON

**Files:**
- Create: `src/store/row-mapper.ts`, `src/store/canonical-json.ts`
- Test: `test/unit/store/row-mapper.spec.ts`, `test/unit/store/canonical-json.spec.ts`

**Interfaces:**
- Consumes: `JobAttributes` type from `src/job` (type-only import).
- Produces:
  - `type JobRecord = Omit<JobAttributes, 'pulse'>`
  - `type ColumnValues = Array<[column: string, value: unknown]>`
  - `toColumnValues(attrs: Partial<JobAttributes>): ColumnValues`
  - `fromRow(row: Record<string, unknown>): JobRecord`
  - `canonicalJson(value: unknown): string`

- [ ] **Step 1: Write the failing tests**

`test/unit/store/row-mapper.spec.ts`:

```ts
import { JobAttributes } from '../../../src/job';
import { fromRow, toColumnValues } from '../../../src/store/row-mapper';

const attrs = (value: Record<string, unknown>) => value as unknown as Partial<JobAttributes>;

describe('toColumnValues', () => {
  test('maps present attributes to snake_case columns', () => {
    const nextRunAt = new Date('2026-01-01T00:00:00Z');
    expect(toColumnValues(attrs({ name: 'a', type: 'normal', nextRunAt, runCount: 2 }))).toEqual([
      ['name', 'a'],
      ['type', 'normal'],
      ['next_run_at', nextRunAt],
      ['run_count', 2],
    ]);
  });

  test('skips absent attributes so updates leave those columns alone', () => {
    expect(toColumnValues(attrs({ name: 'a' }))).toEqual([['name', 'a']]);
  });

  test('writes present-but-undefined and null attributes as NULL', () => {
    expect(toColumnValues(attrs({ lockedAt: undefined, failedAt: null }))).toEqual([
      ['locked_at', null],
      ['failed_at', null],
    ]);
  });

  test('treats undefined as absent for NOT NULL columns with a default', () => {
    expect(toColumnValues(attrs({ priority: undefined, disabled: undefined, shouldSaveResult: undefined }))).toEqual(
      []
    );
  });

  test('serialises json columns, so Dates inside data become ISO strings', () => {
    const when = new Date('2026-02-03T04:05:06.000Z');
    expect(toColumnValues(attrs({ data: { when, list: [1, 2] }, backoff: { type: 'fixed', delay: 5 } }))).toEqual([
      ['data', '{"when":"2026-02-03T04:05:06.000Z","list":[1,2]}'],
      ['backoff', '{"type":"fixed","delay":5}'],
    ]);
  });

  test('converts numeric and string dates to Date', () => {
    const [[, start], [, end]] = toColumnValues(attrs({ startDate: 0, endDate: '2026-01-01T00:00:00Z' }));
    expect(start).toEqual(new Date(0));
    expect(end).toEqual(new Date('2026-01-01T00:00:00Z'));
  });

  test('does not persist attributes without a column', () => {
    expect(toColumnValues(attrs({ _id: 'x', uniqueKey: { a: 1 }, uniqueOpts: { insertOnly: true }, extra: 1 }))).toEqual(
      []
    );
  });
});

describe('fromRow', () => {
  test('maps columns back to attributes and id to _id', () => {
    const nextRunAt = new Date();
    expect(
      fromRow({ id: 'abc', name: 'a', type: 'normal', priority: 0, next_run_at: nextRunAt, data: { x: 1 } })
    ).toEqual({ _id: 'abc', name: 'a', type: 'normal', priority: 0, nextRunAt, data: { x: 1 } });
  });

  test('leaves NULL columns out, so counters read as undefined', () => {
    const record = fromRow({ id: 'abc', name: 'a', run_count: null, locked_at: null, unique_key: 'k' });
    expect(record).toEqual({ _id: 'abc', name: 'a' });
    expect('runCount' in record).toBe(false);
  });
});
```

`test/unit/store/canonical-json.spec.ts`:

```ts
import { canonicalJson } from '../../../src/store/canonical-json';

describe('canonicalJson', () => {
  test('ignores key order, recursively', () => {
    expect(canonicalJson({ b: 1, a: { d: 2, c: 3 } })).toBe(canonicalJson({ a: { c: 3, d: 2 }, b: 1 }));
    expect(canonicalJson({ b: 1, a: { d: 2, c: 3 } })).toBe('{"a":{"c":3,"d":2},"b":1}');
  });

  test('keeps array order', () => {
    expect(canonicalJson({ a: [2, 1] })).not.toBe(canonicalJson({ a: [1, 2] }));
  });

  test('serialises Dates as ISO strings', () => {
    expect(canonicalJson({ at: new Date('2026-01-01T00:00:00Z') })).toBe('{"at":"2026-01-01T00:00:00.000Z"}');
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `npx jest test/unit/store/row-mapper.spec.ts test/unit/store/canonical-json.spec.ts`
Expected: FAIL — cannot find modules.

- [ ] **Step 3: Implement**

`src/store/row-mapper.ts`:

```ts
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
 * included, mirroring Mongo's `$set`: absent attributes keep their stored value.
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
```

`src/store/canonical-json.ts`:

```ts
const sortKeys = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value && typeof value === 'object' && !(value instanceof Date)) {
    const source = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.keys(source)
        .sort()
        .map((key) => [key, sortKeys(source[key])])
    );
  }
  return value;
};

/** JSON with object keys sorted at every level, so equal keys compare equal as text. */
export const canonicalJson = (value: unknown): string => JSON.stringify(sortKeys(value));
```

- [ ] **Step 4: Run to verify pass**

Run: `npx jest test/unit/store/row-mapper.spec.ts test/unit/store/canonical-json.spec.ts`
Expected: PASS (12 tests).

- [ ] **Step 5: Commit**

```bash
git add src/store/row-mapper.ts src/store/canonical-json.ts test/unit/store/row-mapper.spec.ts test/unit/store/canonical-json.spec.ts
git commit -m "feat(store): map job attributes to and from table rows"
```

---

### Task 4: Typed filter and sort compiler

**Files:**
- Create: `src/store/filter.ts`
- Test: `test/unit/store/filter.spec.ts`

**Interfaces:**
- Consumes: `pulseJobsDdl` (Task 2), `createTestDb` (Task 1), `PulseError`.
- Produces:
  - `type DateCond = { isNull?: boolean; lt?: Date; lte?: Date; gt?: Date; gte?: Date }`
  - `type JobStatus = 'scheduled' | 'running' | 'completed' | 'failed'`
  - `interface JobFilter { ids?: string[]; name?: string | string[]; nameNotIn?: string[]; nameContains?: string; status?: JobStatus; recurring?: boolean; disabled?: boolean; data?: Record<string, unknown>; nextRunAt?: DateCond; lockedAt?: DateCond; lastFinishedAt?: DateCond; failedAt?: DateCond }`
  - `type JobSortField = 'nextRunAt' | 'lastRunAt' | 'lastFinishedAt' | 'failedAt' | 'priority' | 'name'`
  - `type JobSort = Partial<Record<JobSortField, 'asc' | 'desc'>>`
  - `compileWhere(filter: JobFilter, params: unknown[]): string` — pushes values onto `params`, returns a SQL boolean expression (`TRUE` for an empty filter)
  - `compileOrderBy(sort: JobSort): string` — `''` or `ORDER BY …`
  - `escapeLike(text: string): string`

- [ ] **Step 1: Write the failing tests**

`test/unit/store/filter.spec.ts` (runs the compiled SQL against real rows, so it checks semantics, not strings):

```ts
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
```

- [ ] **Step 2: Run to verify failure**

Run: `npx jest test/unit/store/filter.spec.ts`
Expected: FAIL — cannot find module `filter`.

- [ ] **Step 3: Implement**

`src/store/filter.ts`:

```ts
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
```

- [ ] **Step 4: Run to verify pass**

Run: `npx jest test/unit/store/filter.spec.ts`
Expected: PASS (all tests). If the `status scheduled` case fails, check the seed: `failed` and `recurring_100%_x` have a future `next_run_at` and no lock, so they are "scheduled" by the spec's definition — that overlap is intended.

- [ ] **Step 5: Commit**

```bash
git add src/store/filter.ts test/unit/store/filter.spec.ts
git commit -m "feat(store): compile typed job filters and sorts to SQL"
```

---

### Task 5: PgJobStore — writes and reads

**Files:**
- Create: `src/store/pg-job-store.ts`, `src/store/index.ts`
- Test: `test/unit/store/pg-job-store.spec.ts`

**Interfaces:**
- Consumes: Tasks 2–4.
- Produces (class `PgJobStore`):
  - `constructor(pool: Pool, tableName?: string)` (default `'pulse_jobs'`; throws on invalid name)
  - `ensureSchema(): Promise<void>`
  - `insert(values: ColumnValues): Promise<SavedRef>`
  - `update(id: string, values: ColumnValues): Promise<SavedRef | undefined>` (undefined when the row is gone)
  - `upsertSingle(values: ColumnValues, keepExistingNextRunAt: boolean): Promise<SavedRef>`
  - `upsertUnique(values: ColumnValues, insertOnly: boolean): Promise<SavedRef>` (`values` must include `['unique_key', string]`)
  - `find(filter?: JobFilter, options?: FindOptions): Promise<JobRecord[]>`
  - `count(filter?: JobFilter): Promise<number>`
  - `delete(filter: JobFilter): Promise<number>`
  - `setDisabled(filter: JobFilter, disabled: boolean): Promise<number>` (counts rows whose flag changed)
  - `unlock(ids: string[]): Promise<void>`
  - `interface SavedRef { _id: string; nextRunAt: Date | null }`
  - `interface FindOptions { sort?: JobSort; limit?: number; skip?: number }`
- `src/store/index.ts` exports everything public from `table`, `ddl`, `row-mapper`, `canonical-json`, `filter`, `pg-job-store`.

- [ ] **Step 1: Write the failing tests**

`test/unit/store/pg-job-store.spec.ts`:

```ts
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
```

- [ ] **Step 2: Run to verify failure**

Run: `npx jest test/unit/store/pg-job-store.spec.ts`
Expected: FAIL — cannot find module `../../../src/store`.

- [ ] **Step 3: Implement**

`src/store/pg-job-store.ts`:

```ts
import type { Pool } from 'pg';
import { pulseJobsDdl } from './ddl';
import { compileOrderBy, compileWhere, JobFilter, JobSort } from './filter';
import { ColumnValues, fromRow, JobRecord } from './row-mapper';
import { quoteTableName } from './table';

export interface SavedRef {
  _id: string;
  nextRunAt: Date | null;
}

export interface FindOptions {
  sort?: JobSort;
  limit?: number;
  skip?: number;
}

const RETURNING_REF = 'RETURNING id, next_run_at';

const toSavedRef = (row: { id: string; next_run_at: Date | null }): SavedRef => ({
  _id: row.id,
  nextRunAt: row.next_run_at,
});

/** Every SQL statement Pulse runs. Each method is one statement on the caller's pool. */
export class PgJobStore {
  private readonly table: string;

  constructor(
    private readonly pool: Pool,
    private readonly tableName = 'pulse_jobs'
  ) {
    this.table = quoteTableName(tableName);
  }

  async ensureSchema(): Promise<void> {
    await this.pool.query(pulseJobsDdl(this.tableName));
  }

  async insert(values: ColumnValues): Promise<SavedRef> {
    const { columns, placeholders, params } = this.insertParts(values);
    const { rows } = await this.pool.query(
      `INSERT INTO ${this.table} (${columns}) VALUES (${placeholders}) ${RETURNING_REF}`,
      params
    );
    return toSavedRef(rows[0]);
  }

  async update(id: string, values: ColumnValues): Promise<SavedRef | undefined> {
    const params: unknown[] = [id];
    const assignments = values.map(([column, value]) => {
      params.push(value);
      return `${column} = $${params.length}`;
    });
    const { rows } = await this.pool.query(
      `UPDATE ${this.table} SET ${assignments.join(', ')} WHERE id = $1 ${RETURNING_REF}`,
      params
    );
    return rows[0] ? toSavedRef(rows[0]) : undefined;
  }

  async upsertSingle(values: ColumnValues, keepExistingNextRunAt: boolean): Promise<SavedRef> {
    const { columns, placeholders, params } = this.insertParts(values);
    const updates = values
      .map(([column]) => column)
      .filter((column) => !(keepExistingNextRunAt && column === 'next_run_at'))
      .map((column) => `${column} = EXCLUDED.${column}`);
    const { rows } = await this.pool.query(
      `INSERT INTO ${this.table} (${columns}) VALUES (${placeholders})
       ON CONFLICT (name) WHERE type = 'single' DO UPDATE SET ${updates.join(', ')}
       ${RETURNING_REF}`,
      params
    );
    return toSavedRef(rows[0]);
  }

  async upsertUnique(values: ColumnValues, insertOnly: boolean): Promise<SavedRef> {
    const { columns, placeholders, params } = this.insertParts(values);
    const conflict = `ON CONFLICT (name, unique_key) WHERE unique_key IS NOT NULL`;
    const action = insertOnly
      ? 'DO NOTHING'
      : `DO UPDATE SET ${values.map(([column]) => `${column} = EXCLUDED.${column}`).join(', ')}`;
    const { rows } = await this.pool.query(
      `INSERT INTO ${this.table} (${columns}) VALUES (${placeholders}) ${conflict} ${action} ${RETURNING_REF}`,
      params
    );
    if (rows[0]) {
      return toSavedRef(rows[0]);
    }

    // insertOnly hit an existing row: return that row, as Mongo's $setOnInsert upsert did.
    const name = values.find(([column]) => column === 'name')?.[1];
    const uniqueKey = values.find(([column]) => column === 'unique_key')?.[1];
    const existing = await this.pool.query(
      `SELECT id, next_run_at FROM ${this.table} WHERE name = $1 AND unique_key = $2`,
      [name, uniqueKey]
    );
    return toSavedRef(existing.rows[0]);
  }

  async find(filter: JobFilter = {}, { sort = {}, limit = 0, skip = 0 }: FindOptions = {}): Promise<JobRecord[]> {
    const params: unknown[] = [];
    let sql = `SELECT * FROM ${this.table} WHERE ${compileWhere(filter, params)} ${compileOrderBy(sort)}`;
    if (limit > 0) {
      params.push(limit);
      sql += ` LIMIT $${params.length}`;
    }
    if (skip > 0) {
      params.push(skip);
      sql += ` OFFSET $${params.length}`;
    }
    const { rows } = await this.pool.query(sql, params);
    return rows.map(fromRow);
  }

  async count(filter: JobFilter = {}): Promise<number> {
    const params: unknown[] = [];
    const { rows } = await this.pool.query(
      `SELECT count(*)::int AS count FROM ${this.table} WHERE ${compileWhere(filter, params)}`,
      params
    );
    return rows[0].count;
  }

  async delete(filter: JobFilter): Promise<number> {
    const params: unknown[] = [];
    const result = await this.pool.query(`DELETE FROM ${this.table} WHERE ${compileWhere(filter, params)}`, params);
    return result.rowCount ?? 0;
  }

  async setDisabled(filter: JobFilter, disabled: boolean): Promise<number> {
    const params: unknown[] = [];
    const where = compileWhere(filter, params);
    params.push(disabled);
    const flag = `$${params.length}`;
    const result = await this.pool.query(
      `UPDATE ${this.table} SET disabled = ${flag} WHERE ${where} AND disabled IS DISTINCT FROM ${flag}`,
      params
    );
    return result.rowCount ?? 0;
  }

  async unlock(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }
    await this.pool.query(`UPDATE ${this.table} SET locked_at = NULL WHERE id = ANY($1::uuid[])`, [ids]);
  }

  private insertParts(values: ColumnValues) {
    return {
      columns: values.map(([column]) => column).join(', '),
      placeholders: values.map((_, index) => `$${index + 1}`).join(', '),
      params: values.map(([, value]) => value),
    };
  }
}
```

`src/store/index.ts`:

```ts
export * from './canonical-json';
export * from './ddl';
export * from './filter';
export * from './pg-job-store';
export * from './row-mapper';
export * from './table';
```

- [ ] **Step 4: Run to verify pass**

Run: `npx jest test/unit/store`
Expected: PASS (all store tests).

- [ ] **Step 5: Commit**

```bash
git add src/store/pg-job-store.ts src/store/index.ts test/unit/store/pg-job-store.spec.ts
git commit -m "feat(store): add PgJobStore reads, writes and upserts"
```

---

### Task 6: PgJobStore — locking and resume

**Files:**
- Modify: `src/store/pg-job-store.ts`
- Test: `test/unit/store/pg-job-store-locking.spec.ts`

**Interfaces:**
- Consumes: Task 5.
- Produces (methods on `PgJobStore`):
  - `lockNext(name: string, options: LockNextOptions): Promise<JobRecord | undefined>` with `interface LockNextOptions { now: Date; nextScanAt: Date; lockDeadline: Date; sort: JobSort }`
  - `lockById(id: string, nextRunAt: Date | null | undefined, now: Date): Promise<JobRecord | undefined>`
  - `resumeUnfinished(now: Date, lockDeadlines: Record<string, Date>, defaultLockDeadline: Date): Promise<number>`

- [ ] **Step 1: Write the failing tests**

`test/unit/store/pg-job-store-locking.spec.ts`:

```ts
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
```

- [ ] **Step 2: Run to verify failure**

Run: `npx jest test/unit/store/pg-job-store-locking.spec.ts`
Expected: FAIL — `store.lockNext is not a function` (TypeScript: property does not exist).

- [ ] **Step 3: Implement**

Add to `src/store/pg-job-store.ts` (the interface next to `FindOptions`, the methods inside the class after `unlock`):

```ts
export interface LockNextOptions {
  now: Date;
  nextScanAt: Date;
  lockDeadline: Date;
  sort: JobSort;
}
```

```ts
  /**
   * Locks the next due job of `name`. SKIP LOCKED lets concurrent pollers
   * pass over a row another poller is locking instead of waiting or taking it too.
   */
  async lockNext(name: string, { now, nextScanAt, lockDeadline, sort }: LockNextOptions): Promise<JobRecord | undefined> {
    const { rows } = await this.pool.query(
      `UPDATE ${this.table} SET locked_at = $1
       WHERE id = (
         SELECT id FROM ${this.table}
         WHERE name = $2 AND NOT disabled
           AND ((locked_at IS NULL AND next_run_at <= $3) OR locked_at <= $4)
         ${compileOrderBy(sort)}
         LIMIT 1
         FOR UPDATE SKIP LOCKED)
       RETURNING *`,
      [now, name, nextScanAt, lockDeadline]
    );
    return rows[0] ? fromRow(rows[0]) : undefined;
  }

  async lockById(id: string, nextRunAt: Date | null | undefined, now: Date): Promise<JobRecord | undefined> {
    const { rows } = await this.pool.query(
      `UPDATE ${this.table} SET locked_at = $1
       WHERE id = $2 AND locked_at IS NULL AND next_run_at IS NOT DISTINCT FROM $3 AND NOT disabled
       RETURNING *`,
      [now, id, nextRunAt ?? null]
    );
    return rows[0] ? fromRow(rows[0]) : undefined;
  }

  /**
   * Makes interrupted work runnable again. Only locks older than the job's lock
   * lifetime are released, so jobs running on other processes are left alone.
   */
  async resumeUnfinished(now: Date, lockDeadlines: Record<string, Date>, defaultLockDeadline: Date): Promise<number> {
    const names = Object.keys(lockDeadlines);
    const result = await this.pool.query(
      `WITH deadline(name, at) AS (SELECT * FROM unnest($2::text[], $3::timestamptz[]))
       UPDATE ${this.table} AS job
       SET locked_at = NULL, last_modified_by = NULL, last_run_at = NULL, next_run_at = $1
       WHERE (job.locked_at IS NOT NULL
              AND job.locked_at <= COALESCE((SELECT at FROM deadline WHERE deadline.name = job.name), $4)
              AND job.next_run_at IS NOT NULL
              AND (job.run_count IS NOT DISTINCT FROM job.finished_count OR job.last_finished_at IS NULL))
          OR (job.locked_at IS NULL
              AND job.last_finished_at IS NULL
              AND job.next_run_at <= $1)`,
      [now, names, names.map((name) => lockDeadlines[name]), defaultLockDeadline]
    );
    return result.rowCount ?? 0;
  }
```

- [ ] **Step 4: Run to verify pass**

Run: `npx jest test/unit/store`
Expected: PASS (all store tests, including the 40-job concurrency test).

- [ ] **Step 5: Commit**

```bash
git add src/store/pg-job-store.ts test/unit/store/pg-job-store-locking.spec.ts
git commit -m "feat(store): lock jobs with SKIP LOCKED and resume only expired locks"
```

---

### Task 7: Wire Pulse and Job to the store, port the suite

This task swaps every `_collection` call at once. The `Pulse` class imports all of its methods, so a partial swap would not compile.

**Files:**
- Create: `src/pulse/pg.ts`, `src/pulse/ensure-schema.ts`, `src/pulse/resume-unfinished-jobs.ts`
- Modify: `src/pulse/index.ts`, `src/pulse/save-job.ts`, `src/pulse/jobs.ts`, `src/pulse/count-jobs.ts`, `src/pulse/cancel.ts`, `src/pulse/disable.ts`, `src/pulse/enable.ts`, `src/pulse/purge.ts`, `src/pulse/sort.ts`, `src/pulse/close.ts`, `src/pulse/stop.ts`, `src/pulse/start.ts`, `src/pulse/resume-on-restart.ts`, `src/pulse/find-and-lock-next-job.ts`, `src/job/index.ts`, `src/job/unique.ts`, `src/job/remove.ts`, `src/job/fetch-status.ts`, `src/utils/create-job.ts`, `src/utils/process-jobs.ts:138-158`, `src/index.ts`
- Delete: `src/pulse/database.ts`, `src/pulse/db-init.ts`, `src/pulse/mongo.ts`, `src/pulse/has-mongo-protocol.ts`, `test/helpers/mock.helper.ts`
- Test: `test/unit/pulse.spec.ts`

**Interfaces:**
- Consumes: `PgJobStore`, `JobFilter`, `JobSort`, `FindOptions`, `JobRecord`, `toColumnValues`, `canonicalJson` (Tasks 2–6).
- Produces:
  - `PulseConfig.pg?: { pool: Pool; table?: string }`, `PulseConfig.autoSchema?: boolean`, `PulseConfig.sort?: JobSort`
  - `pulse.pg(pool: Pool, table?: string): Pulse`; `pulse.ensureSchema(): Promise<void>`; `pulse._store: PgJobStore`; `pulse._resumeUnfinishedJobs(): Promise<number>`
  - `pulse.jobs(filter?: JobFilter, options?: FindOptions): Promise<Job[]>`; `countJobs(filter?)`; `cancel(filter)`; `disable(filter?)`; `enable(filter?)` → `Promise<number>`
  - `job.attrs._id: string`; `job.unique(key: Record<string, unknown>, options?: { insertOnly?: boolean }): Job`
  - `createJob(pulse: Pulse, record: JobRecord): Job`

- [ ] **Step 1: Rewrite the test suite against Postgres (RED)**

Edit `test/unit/pulse.spec.ts` in four places. Blocks not mentioned stay exactly as they are: lines 81–192 (`Test processEvery` through `Test defaultLockLifetime`), lines 373–502 (`Test job methods` → `Test create method`, `Test define method`, `Test every method` — their `jobs({ name })` calls are already valid `JobFilter`s), and lines 540–560 (`Test schedule method`).

**(a)** Replace lines 1–80 (imports, setup, and the two `Test mongo…` blocks) with:

```ts
/* eslint-disable no-console,no-unused-expressions,@typescript-eslint/no-unused-expressions */

import { Pulse } from '../../src';
import { Job } from '../../src/job';
import { PgJobStore } from '../../src/store';
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

```

**(b)** Replace lines 193–372 (`Test sort` and `Test resumeOnRestart`) with the block below. The old resume tests that saved `nextRunAt = null` and asserted it was "not null" afterwards passed only because `jobs()` builds a `Job`, whose constructor turns a missing `nextRunAt` into now. The "interrupted recurring job" test passed because `save()` overwrote `lastModifiedBy`. They are replaced by tests that read the stored row.

```ts
    describe('Test sort', () => {
      test('sets the default sort', () => {
        expect(globalPulseInstance._sort).toEqual({ nextRunAt: 'asc', priority: 'desc' });
      });

      test('sets the custom sort', () => {
        globalPulseInstance.sort({ nextRunAt: 'desc' });
        expect(globalPulseInstance._sort).toEqual({ nextRunAt: 'desc' });
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
```

**(c)** Replace lines 503–539 (`Test countJobs`) with:

```ts
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
```

**(d)** Immediately before the file's final `});` (after `Test schedule method`'s closing `});`), insert:

```ts

  describe('Test query and persistence API', () => {
    test('jobs() applies sort, limit and skip', async () => {
      const base = Date.now() + 60_000;
      for (const [name, offset] of [
        ['a', 1],
        ['b', 2],
        ['c', 3],
      ] as const) {
        await globalPulseInstance.create(name, {}).schedule(new Date(base + offset * 1000)).save();
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
      globalPulseInstance.processEvery('1 second');
      await globalPulseInstance.start();

      await globalPulseInstance.now('quick');
      await delay(2500);

      expect(ran).toHaveBeenCalledTimes(1);
      const stored = await storedJob('quick');
      expect(stored.lastFinishedAt).toBeDefined();
      expect(stored.lockedAt).toBeUndefined();
    });
  });
```

- [ ] **Step 2: Run to verify failure**

Run: `npx jest test/unit/pulse.spec.ts`
Expected: FAIL — TypeScript errors such as `Object literal may only specify known properties, and 'pg' does not exist in type 'PulseConfig'` and `Property '_store' does not exist on type 'Pulse'`.

- [ ] **Step 3: Change the Job types and job methods**

`src/job/index.ts`:
- Delete `import * as mongodb from 'mongodb';`.
- In `JobAttributes`, change `_id: mongodb.ObjectId;` to `_id: string;`.
- Replace
  ```ts
    uniqueQuery?: any;
    uniqueOpts?: {
      insertOnly: boolean;
    };
  ```
  with
  ```ts
    uniqueKey?: Record<string, unknown>;
    uniqueOpts?: {
      insertOnly?: boolean;
    };
  ```
- Change the constructor parameter type `Modify<JobAttributes<T>, { _id?: mongodb.ObjectId }>` to `Modify<JobAttributes<T>, { _id?: string }>`.

`src/job/unique.ts` (whole file):

```ts
import { Job } from '.';

export type UniqueMethod = (key: Record<string, unknown>, options?: { insertOnly?: boolean }) => Job;
/**
 * Upsert on `key` instead of always inserting: a later save with an equal key
 * (compared as key-sorted JSON) updates that job, or keeps it with `insertOnly`.
 * @name Job#unique
 * @function
 * @param key identity of the job, e.g. { patientId }
 * @param options { insertOnly: true } keeps the existing job unchanged
 */
export const unique: UniqueMethod = function (this: Job, key, options?) {
  this.attrs.uniqueKey = key;
  this.attrs.uniqueOpts = options;
  return this;
};
```

`src/job/remove.ts` — change the body to:

```ts
export const remove: RemoveMethod = async function (this: Job) {
  return this.pulse.cancel({ ids: [this.attrs._id] });
};
```

and its JSDoc line `Remove the job from MongoDB` to `Remove the job from the database`.

`src/job/fetch-status.ts` — replace the first lines of the function body:

```ts
export const fetchStatus: FetchStatusMethod = async function (this: Job) {
  const [dbJob] = await this.pulse._store.find({ ids: [this.attrs._id] });
  if (!dbJob) {
    return {
      status: false,
      reason: `job with id ${this.attrs._id} not found in database`,
    };
  }

  this.attrs.lastRunAt = dbJob.lastRunAt;
  this.attrs.lockedAt = dbJob.lockedAt;
  this.attrs.lastFinishedAt = dbJob.lastFinishedAt;

  return {
    status: true,
    lastRunAt: this.attrs.lastRunAt,
    lockedAt: this.attrs.lockedAt ?? undefined,
    lastFinishedAt: this.attrs.lastFinishedAt,
  };
};
```

`src/utils/create-job.ts` (whole file):

```ts
import { Job } from '../job';
import { Pulse } from '../pulse';
import { JobRecord } from '../store';

/**
 * Create Job object from a stored record
 * @param {Object} pulse instance of Pulse
 * @param {Object} record job attributes read from the database
 * @returns {Job} returns created job
 */
export const createJob = (pulse: Pulse, record: JobRecord): Job => new Job({ ...record, pulse });
```

- [ ] **Step 4: Point the Pulse methods at the store**

New `src/pulse/pg.ts`:

```ts
import createDebugger from 'debug';
import type { Pool } from 'pg';
import { Pulse } from '.';
import { PgJobStore } from '../store';

const debug = createDebugger('pulse:pg');

export type PgMethod = (pool: Pool, table?: string) => Pulse;
/**
 * Attach a caller-owned pg Pool. Emits 'ready' asynchronously, after the
 * schema is created when `autoSchema` is on, so listeners attached right
 * after the constructor still fire.
 * @name Pulse#pg
 * @function
 * @param pool pg Pool; Pulse never ends it
 * @param [table] table name, `table` or `schema.table`. Defaults to `pulse_jobs`
 */
export const pg: PgMethod = function (this: Pulse, pool, table = 'pulse_jobs') {
  debug('attaching postgres store using table [%s]', table);
  this._store = new PgJobStore(pool, table);

  const schemaReady = this._autoSchema ? this._store.ensureSchema() : Promise.resolve();
  schemaReady
    .then(() => this.emit('ready'))
    .catch((error) => {
      debug('schema creation failed');
      this.emit('error', error);
    });

  return this;
};
```

New `src/pulse/ensure-schema.ts`:

```ts
import { Pulse } from '.';

export type EnsureSchemaMethod = () => Promise<void>;
/**
 * Create the jobs table and its indexes if they do not exist.
 * @name Pulse#ensureSchema
 * @function
 */
export const ensureSchema: EnsureSchemaMethod = async function (this: Pulse) {
  await this._store.ensureSchema();
};
```

New `src/pulse/resume-unfinished-jobs.ts`:

```ts
import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:resumeUnfinishedJobs');

/**
 * Release locks older than each job's lock lifetime and reschedule
 * never-run jobs that are past due. Live locks held by other processes are
 * left alone. Called by start() when resumeOnRestart is on.
 * @access protected
 */
export const resumeUnfinishedJobs = async function (this: Pulse): Promise<number> {
  const now = new Date();
  const lockDeadlines: Record<string, Date> = {};
  for (const [name, definition] of Object.entries<{ lockLifetime: number }>(this._definitions)) {
    lockDeadlines[name] = new Date(now.getTime() - definition.lockLifetime);
  }

  const resumed = await this._store.resumeUnfinished(
    now,
    lockDeadlines,
    new Date(now.getTime() - this._defaultLockLifetime)
  );
  if (resumed > 0) {
    debug('resumed %d unfinished jobs (%s)', resumed, now.toISOString());
  }
  return resumed;
};
```

`src/pulse/resume-on-restart.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:resumeOnRestart');

export type ResumeOnRestartMethod = (resume?: boolean) => Pulse;

/**
 * Set the resume on restart flag. When on, start() releases expired locks and
 * reschedules past-due jobs that never ran before processing begins.
 * @name Pulse#resumeOnRestart
 * @function
 * @param resume resume on restart
 */
export const resumeOnRestart: ResumeOnRestartMethod = function (this: Pulse, resume = true) {
  debug('Pulse.resumeOnRestart(%s)', resume);
  this._resumeOnRestart = resume;
  return this;
};
```

`src/pulse/start.ts` — after `await this._ready;` insert:

```ts
  if (this._resumeOnRestart) {
    await this._resumeUnfinishedJobs();
  }
```

`src/pulse/stop.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:stop');

export type StopMethod = () => Promise<void>;
/**
 * Clear the interval that processes the jobs and release this instance's locks
 * @name Pulse#stop
 * @function
 * @returns resolves when the locks are released
 */
export const stop: StopMethod = async function (this: Pulse) {
  debug('Pulse.stop called, clearing interval for processJobs()');
  clearInterval(this._processInterval);
  this._processInterval = undefined;

  const jobIds = this._lockedJobs.map((job) => job.attrs._id);
  if (this._store && jobIds.length > 0) {
    debug('about to unlock jobs with ids: %O', jobIds);
    await this._store.unlock(jobIds);
  }
  this._lockedJobs = [];
};
```

`src/pulse/close.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:close');

export type CloseMethod = () => Promise<Pulse>;
/**
 * Kept for API compatibility. The pg Pool belongs to the caller, so there is
 * nothing for Pulse to close; end the pool yourself.
 * @name Pulse#close
 * @function
 */
export const close: CloseMethod = async function (this: Pulse) {
  debug('close called; the pg pool is owned by the caller and is left open');
  return this;
};
```

`src/pulse/find-and-lock-next-job.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';
import { Job } from '../job';
import { createJob } from '../utils';

const debug = createDebugger('pulse:internal:_findAndLockNextJob');

/**
 * Find and lock jobs
 * @name Pulse#findAndLockNextJob
 * @function
 * @param jobName name of job to try to lock
 * @param definition definition used to tell how job is run
 * @access protected
 * @caller jobQueueFilling() only
 */
export const findAndLockNextJob = async function (
  this: Pulse,
  jobName: string,
  definition: { lockLifetime: number }
): Promise<Job | undefined> {
  debug('_findAndLockNextJob(%s, [Function])', jobName);
  const record = await this._store.lockNext(jobName, {
    now: new Date(),
    nextScanAt: this._nextScanAt,
    lockDeadline: new Date(Date.now() - definition.lockLifetime),
    sort: this._sort,
  });

  if (!record) {
    return undefined;
  }
  debug('found a job available to lock, creating a new job on Pulse with id [%s]', record._id);
  return createJob(this, record);
};
```

`src/pulse/save-job.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';
import { Job } from '../job';
import { canonicalJson, SavedRef, toColumnValues } from '../store';
import { JobError, processJobs } from '../utils';

const debug = createDebugger('pulse:saveJob');

/**
 * Store the saved id and nextRunAt on the job, and process it immediately if it
 * would have been picked up by a scan that already happened.
 * @access private
 */
const processDbResult = async function (this: Pulse, job: Job, saved: SavedRef | undefined): Promise<Job> {
  if (!saved) {
    debug('[%s:%s] job no longer exists, nothing was saved', job.attrs.name, job.attrs._id);
    return job;
  }

  job.attrs._id = saved._id;
  job.attrs.nextRunAt = saved.nextRunAt;

  if (job.attrs.nextRunAt && job.attrs.nextRunAt < this._nextScanAt) {
    debug('[%s:%s] job would have ran by nextScanAt, processing the job immediately', job.attrs.name, saved._id);
    await processJobs.call(this, job);
  }

  return job;
};

export type SaveJobMethod = (job: Job) => Promise<Job>;
/**
 * Save the properties on a job to the database. Only attributes present on the
 * job are written, so columns it does not carry keep their stored values.
 * @name Pulse#saveJob
 * @function
 * @param job job to save
 * @returns resolves when job is saved or errors
 */
export const saveJob: SaveJobMethod = async function (this: Pulse, job) {
  if (!this._store) {
    throw new JobError('A db must be set up before you can save a job');
  }

  const id = job.attrs._id;
  const { uniqueKey, uniqueOpts } = job.attrs;

  const props = job.toJSON();
  props.lastModifiedBy = this._name;
  const values = toColumnValues(props);
  debug('[job %s] saving columns %O', id, values);

  if (id) {
    return processDbResult.call(this, job, await this._store.update(id, values));
  }

  if (props.type === 'single') {
    // A past nextRunAt must not move an existing 'single' job's schedule.
    const keepExistingNextRunAt = Boolean(props.nextRunAt && props.nextRunAt <= new Date());
    return processDbResult.call(this, job, await this._store.upsertSingle(values, keepExistingNextRunAt));
  }

  if (uniqueKey) {
    values.push(['unique_key', canonicalJson(uniqueKey)]);
    return processDbResult.call(this, job, await this._store.upsertUnique(values, Boolean(uniqueOpts?.insertOnly)));
  }

  return processDbResult.call(this, job, await this._store.insert(values));
};
```

`src/pulse/jobs.ts` (whole file):

```ts
import { Pulse } from '.';
import { Job } from '../job';
import { FindOptions, JobFilter } from '../store';
import { createJob } from '../utils';

export type JobsMethod = (filter?: JobFilter, options?: FindOptions) => Promise<Job[]>;

/**
 * Finds all jobs matching `filter`
 * @name Pulse#jobs
 * @function
 * @param [filter] typed job filter
 * @param [options] sort, limit and skip
 */
export const jobs: JobsMethod = async function (this: Pulse, filter = {}, options = {}) {
  const records = await this._store.find(filter, options);
  return records.map((record) => createJob(this, record));
};
```

`src/pulse/count-jobs.ts` (whole file):

```ts
import { Pulse } from '.';
import { JobFilter } from '../store';

export type CountJobsMethod = (filter?: JobFilter) => Promise<number>;

/**
 * Counts all jobs matching `filter`
 * @name Pulse#countJobs
 * @function
 * @param [filter] typed job filter
 */
export const countJobs: CountJobsMethod = async function (this: Pulse, filter = {}) {
  return this._store.count(filter);
};
```

`src/pulse/cancel.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';
import { JobFilter } from '../store';

const debug = createDebugger('pulse:cancel');

export type CancelMethod = (filter: JobFilter) => Promise<number>;
/**
 * Cancels any jobs matching `filter` and removes them from the database.
 * @name Pulse#cancel
 * @function
 * @param filter typed job filter
 * @caller client code, Pulse.purge(), Job.remove()
 */
export const cancel: CancelMethod = async function (this: Pulse, filter) {
  debug('attempting to cancel all Pulse jobs', filter);
  const deletedCount = await this._store.delete(filter);
  this.emit('cancel', deletedCount);
  debug('%s jobs cancelled', deletedCount);
  return deletedCount;
};
```

`src/pulse/disable.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';
import { JobFilter } from '../store';

const debug = createDebugger('pulse:disable');

export type DisableMethod = (filter?: JobFilter) => Promise<number>;
/**
 * Disables any jobs matching `filter` by setting the `disabled` flag to `true`
 * @name Pulse#disable
 * @function
 * @param filter typed job filter
 * @returns {Promise<number>} Resolved with the number of jobs whose flag changed.
 */
export const disable: DisableMethod = async function (this: Pulse, filter = {}) {
  debug('attempting to disable all jobs matching filter', filter);
  const modifiedCount = await this._store.setDisabled(filter, true);
  debug('%s jobs disabled', modifiedCount);
  return modifiedCount;
};
```

`src/pulse/enable.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';
import { JobFilter } from '../store';

const debug = createDebugger('pulse:enable');

export type EnableMethod = (filter?: JobFilter) => Promise<number>;
/**
 * Enables any jobs matching `filter` by setting the `disabled` flag to `false`
 * @name Pulse#enable
 * @function
 * @param filter typed job filter
 * @returns {Promise<number>} Resolved with the number of jobs whose flag changed.
 */
export const enable: EnableMethod = async function (this: Pulse, filter = {}) {
  debug('attempting to enable all jobs matching filter', filter);
  const modifiedCount = await this._store.setDisabled(filter, false);
  debug('%s jobs enabled', modifiedCount);
  return modifiedCount;
};
```

`src/pulse/purge.ts` — change the return line and type:

```ts
export type PurgeMethod = () => Promise<number>;
```

```ts
  return this.cancel({ nameNotIn: definedNames });
```

`src/pulse/sort.ts` (whole file):

```ts
import createDebugger from 'debug';
import { Pulse } from '.';
import { JobSort } from '../store';

const debug = createDebugger('pulse:sort');

export type SortMethod = (sort: JobSort) => Pulse;
/**
 * Set the sort used when picking the next job to lock
 * @name Pulse#sort
 * @function
 * @param sort e.g. { nextRunAt: 'asc', priority: 'desc' }
 */
export const sort: SortMethod = function (this: Pulse, sort) {
  debug('Pulse.sort(%O)', sort);
  this._sort = sort;
  return this;
};
```

`src/utils/process-jobs.ts` — in `lockOnTheFly`, replace the block from `// Query to run against collection to see if we need to lock it` through the closing `}` of `if (resp?.value) { … }` with:

```ts
    // Lock the job only if nobody else has, and it is still due when we expected
    const record = await self._store.lockById(job.attrs._id, job.attrs.nextRunAt, now);

    if (record) {
      const job = createJob(self, record);
      debug('found job [%s:%s] that can be locked on the fly', job.attrs.name, job.attrs._id);
      self._lockedJobs.push(job);
      definitions[job.attrs.name].locked++;
      enqueueJobs(job);
      jobProcessing();
    }
```

Also change the comment `// NOTE: Shouldn't we update the 'lockedAt' value in MongoDB so it can be picked up on restart?` to `// NOTE: Shouldn't we update the 'lockedAt' value in the database so it can be picked up on restart?`.

- [ ] **Step 5: Update the Pulse class**

Delete `src/pulse/database.ts`, `src/pulse/db-init.ts`, `src/pulse/mongo.ts`, `src/pulse/has-mongo-protocol.ts`, `test/helpers/mock.helper.ts`:

```bash
git rm src/pulse/database.ts src/pulse/db-init.ts src/pulse/mongo.ts src/pulse/has-mongo-protocol.ts test/helpers/mock.helper.ts
```

In `src/pulse/index.ts`:

1. Replace `import { AnyError, Collection, MongoClient, MongoClientOptions, Db as MongoDb } from 'mongodb';` with `import type { Pool } from 'pg';`.
2. Delete the imports of `./database`, `./db-init`, `./mongo` and the `getJobsRepo` name from the `./jobs` import (keep `JobsMethod, jobs`).
3. Add imports:
   ```ts
   import { EnsureSchemaMethod, ensureSchema } from './ensure-schema';
   import { PgMethod, pg } from './pg';
   import { resumeUnfinishedJobs } from './resume-unfinished-jobs';
   import { JobSort, PgJobStore } from '../store';
   ```
4. Replace the `PulseConfig` interface with:
   ```ts
   export interface PulseConfig {
     name?: string;
     processEvery?: string;
     maxConcurrency?: number;
     defaultConcurrency?: number;
     lockLimit?: number;
     defaultLockLimit?: number;
     defaultLockLifetime?: number;
     sort?: JobSort;
     pg?: {
       pool: Pool;
       table?: string;
     };
     autoSchema?: boolean;
     resumeOnRestart?: boolean;
   }
   ```
5. In the class fields: delete `_indices: any;`, `_disableAutoIndex: boolean;`, `_mongoUseUnifiedTopology?: boolean;`, `_db!: MongoClient;`, `_mdb!: MongoDb;`, `_collection!: Collection;`. Change `_sort: any;` to `_sort: JobSort;`. Add:
   ```ts
     _resumeUnfinishedJobs = resumeUnfinishedJobs;
     _autoSchema: boolean;
     _store!: PgJobStore;
   ```
6. Constructor: change the signature to `constructor(config: PulseConfig = {})` and its JSDoc to drop `@param cb`. Replace
   ```ts
       this._sort = config.sort || { nextRunAt: 1, priority: -1 };
       this._indices = {
         name: 1,
         ...this._sort,
         priority: -1,
         lockedAt: 1,
         nextRunAt: 1,
         disabled: 1,
       };
       this._disableAutoIndex = config.disableAutoIndex === true;
   ```
   with
   ```ts
       this._sort = config.sort || { nextRunAt: 'asc', priority: 'desc' };
       this._autoSchema = config.autoSchema === true;
   ```
   and replace the last line `this.init(config, cb);` with `this.init(config);`.
7. Getters: delete `get dbInit()`, `get mongo()`, `get database()`, and the whole `Repository method` section with `get getJobsRepo()`. Add next to the other public getters:
   ```ts
     get pg(): PgMethod {
       return this.bindMethod('pg', pg);
     }

     get ensureSchema(): EnsureSchemaMethod {
       return this.bindMethod('ensureSchema', ensureSchema);
     }
   ```
8. Replace the private `init` method with:
   ```ts
     private init(config: PulseConfig) {
       if (config.pg) {
         this.pg(config.pg.pool, config.pg.table);
       }
     }
   ```

`src/index.ts` — add after the existing `export { DefineOptions, JobPriority, Processor } from './pulse/define';`:

```ts
export {
  DateCond,
  FindOptions,
  JobFilter,
  JobSort,
  JobSortField,
  JobStatus,
  PgJobStore,
  pulseJobsDdl,
} from './store';
```

- [ ] **Step 6: Run the ported suite**

Run: `npx jest test/unit/pulse.spec.ts`
Expected: PASS. Then: `npx tsc --noEmit -p test/tsconfig.test.json` — expected: no errors.

If `npx tsc` reports `mongodb` imports anywhere, grep and fix them: `grep -rn "mongodb" src test`. The only remaining hits allowed at this point are comments that say "MongoDB" in prose, which Task 9 rewrites.

- [ ] **Step 7: Run everything**

Run: `npm test && npm run build && npm run lint`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add -A src test
git commit -m "feat!: store jobs in Postgres instead of MongoDB

BREAKING CHANGE: Pulse connects with a pg Pool (config.pg), job ids are uuid
strings, jobs()/countJobs()/cancel()/disable()/enable() take a typed JobFilter,
unique() takes a key object, and resumeOnRestart runs in start() and only
releases expired locks."
```

---

### Task 8: Several Pulse instances on one table

This task checks the multi-process guarantees end to end, with each instance on its own pool as in production. The mechanisms (Tasks 6 and 7) already exist, so these tests are expected to pass on first run. If one fails, that is a real bug: debug it (superpowers:systematic-debugging) and fix the store or processing code, not the test.

**Files:**
- Test: `test/unit/multi-instance.spec.ts`

**Interfaces:**
- Consumes: `Pulse` with `config.pg`, `PgJobStore`, `createTestDb`, `testDatabaseUrl` (Tasks 1, 6, 7).

- [ ] **Step 1: Write the tests**

`test/unit/multi-instance.spec.ts`:

```ts
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
```

- [ ] **Step 2: Run them**

Run: `npx jest test/unit/multi-instance.spec.ts`
Expected: PASS (3 tests). On a failure, record the failing assertion and investigate before changing anything. A duplicate run points at `lockNext`/`lockById`; a stolen lock points at `resumeUnfinished`.

- [ ] **Step 3: Commit**

```bash
git add test/unit/multi-instance.spec.ts
git commit -m "test: run several Pulse instances against one Postgres table"
```

---

### Task 9: Drop MongoDB, package 2.0.0, update docs

**Files:**
- Modify: `package.json`, `package-lock.json` (via npm), `README.md`, `gitbook/README.md`, `gitbook/quick-start.md`, `gitbook/why-pulse.md`, `gitbook/docs/setup-and-config/connection.md`, `gitbook/docs/setup-and-config/config/sort.md`, `gitbook/docs/managing-jobs/{jobs,cancel,disable,enable}.md`, `gitbook/docs/managing-jobs/manually-working/{unique,save,repeatevery-3}.md`, `examples/*.example.ts`

- [ ] **Step 1: Write the failing check**

Add to the end of `test/unit/pg-helper.spec.ts`:

```ts
describe('package', () => {
  test('no longer depends on MongoDB', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const pkg = require('../../package.json');
    const all = { ...pkg.dependencies, ...pkg.devDependencies, ...pkg.peerDependencies };
    expect(Object.keys(all).filter((name) => name.startsWith('mongodb'))).toEqual([]);
    expect(pkg.peerDependencies.pg).toBe('^8');
    expect(pkg.version).toBe('2.0.0');
  });
});
```

Run: `npx jest test/unit/pg-helper.spec.ts`
Expected: FAIL — `mongodb` and `mongodb-memory-server` still listed.

- [ ] **Step 2: Update package.json**

```bash
npm uninstall --no-audit --no-fund mongodb mongodb-memory-server
```

Then edit `package.json` by hand (only these fields; `package-lock.json` was regenerated by npm):
- `"version": "2.0.0"`
- `"description": "The modern PostgreSQL-powered job scheduler library for Node.js"`
- in `keywords`: remove `"mongodb"` (both occurrences) and `"mongodb-scheduler"`; add `"postgres"`, `"postgresql"`, `"postgres-scheduler"`.

Run: `npx jest test/unit/pg-helper.spec.ts` — expected: PASS.
Run: `grep -rn "from 'mongodb\|require('mongodb" src test examples` — expected: no output after Step 4.

- [ ] **Step 3: Docs — connection and breaking changes**

Replace the body of `gitbook/docs/setup-and-config/connection.md` with:

````md
# Connection

Pulse stores jobs in PostgreSQL. Pass it a `pg` Pool you own; Pulse never ends it.

```ts
import { Pool } from 'pg';
import Pulse from '@pulsecron/pulse';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const pulse = new Pulse({ pg: { pool, table: 'pulse_jobs' } });

pulse.on('ready', () => pulse.start());
pulse.on('error', (error) => console.error(error));
```

Or attach the pool after construction: `pulse.pg(pool, 'pulse_jobs')`.

## Creating the table

Either let Pulse create it (`new Pulse({ pg: { pool }, autoSchema: true })`, or `await pulse.ensureSchema()`), or add it to your own migrations using the canonical DDL:

```ts
import { pulseJobsDdl } from '@pulsecron/pulse';
console.log(pulseJobsDdl('pulse_jobs'));
```

`table` may be `table` or `schema.table` (letters, digits, underscores).

## Upgrading from 1.x (MongoDB)

1. Connect with `config.pg` / `pulse.pg()`; `config.db`, `config.mongo`, `database()` and `mongo()` are gone. `close()` no longer closes anything.
2. `job.attrs._id` is a uuid string, not an `ObjectId`.
3. `jobs()`, `countJobs()`, `cancel()`, `disable()` and `enable()` take a `JobFilter`, not a Mongo query. `jobs()` takes `{ sort, limit, skip }` instead of positional arguments. Unknown filter fields throw.
4. `job.unique(key)` takes a plain key object; equal keys (in any key order) upsert the same job.
5. Dates inside `job.attrs.data` and `result` are stored as JSON and come back as ISO strings.
6. `resumeOnRestart` runs inside `start()` and only releases locks older than the job's `lockLifetime`.
7. `disableAutoIndex` is replaced by `autoSchema` (default `false`).
````

In `README.md`, replace every MongoDB connection example with the `Pool` + `config.pg` example above, change "MongoDB" in the tagline/description to "PostgreSQL", and add a section `## Upgrading from 1.x (MongoDB)` with the same seven points (link to the gitbook page for details).

- [ ] **Step 4: Docs and examples — remaining Mongo references**

Apply these replacements (the left column is the Mongo form currently in the file):

| File | Replace | With |
|---|---|---|
| `gitbook/README.md`, `gitbook/why-pulse.md`, `gitbook/quick-start.md` | "MongoDB" (as the storage) | "PostgreSQL"; connection snippet → the `Pool` example from Step 3 |
| `gitbook/docs/managing-jobs/jobs.md` | `pulse.jobs({ name: 'x' }, { nextRunAt: -1 }, 10, 0)` style examples | `pulse.jobs({ name: 'x' }, { sort: { nextRunAt: 'desc' }, limit: 10, skip: 0 })`, plus the `JobFilter` field list from the spec §3 |
| `gitbook/docs/managing-jobs/cancel.md` | `pulse.cancel({ name: 'x' })` with Mongo operators | `pulse.cancel({ name: 'x' })`, `pulse.cancel({ ids: [id] })`; note that `{}` cancels everything and unknown fields throw |
| `gitbook/docs/managing-jobs/disable.md`, `enable.md` | Mongo query argument | `JobFilter` argument, returns the number of jobs whose flag changed |
| `gitbook/docs/managing-jobs/manually-working/unique.md` | `job.unique({ 'data.type': 'x' })` | `job.unique({ type: 'x' })`, `job.unique({ type: 'x' }, { insertOnly: true })` |
| `gitbook/docs/managing-jobs/manually-working/save.md`, `repeatevery-3.md` | "saves to MongoDB" | "saves to the database" |
| `gitbook/docs/setup-and-config/config/sort.md` | `{ nextRunAt: 1, priority: -1 }` | `{ nextRunAt: 'asc', priority: 'desc' }` and the allowed fields `nextRunAt`, `lastRunAt`, `lastFinishedAt`, `failedAt`, `priority`, `name` |
| `examples/*.example.ts` | `new Pulse({ db: { address: …, collection: … } })` | `new Pulse({ pg: { pool: new Pool({ connectionString: process.env.DATABASE_URL }) }, autoSchema: true })` with `import { Pool } from 'pg';` |

Run: `grep -rni "mongo" README.md gitbook examples src`
Expected: hits only in the "Upgrading from 1.x (MongoDB)" sections.

- [ ] **Step 5: Full verification**

Run: `npm run lint && npm run build && npm test`
Expected: lint clean, build succeeds, every suite passes. Paste the final `Tests:` line into the PR description.

- [ ] **Step 6: Commit**

```bash
git add package.json package-lock.json README.md gitbook examples test/unit/pg-helper.spec.ts
git commit -m "chore: drop MongoDB, release as 2.0.0 and document the Postgres setup"
```
