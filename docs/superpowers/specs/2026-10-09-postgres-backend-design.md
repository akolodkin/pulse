# Pulse on Postgres — design

Date: 2026-10-09
Branch: `feat/postgres-backend` (off `fork-release`)
Status: draft for review

## Intent

Move the `@pulsecron/pulse` fork from MongoDB to PostgreSQL as part of LISA's
(`li-call-processor`) exit from MongoDB.

- Postgres only. MongoDB support is removed from the fork, not kept behind an
  interface.
- LISA is the only consumer that matters. The API is shaped around what LISA
  needs, but the full public API is ported (nothing LISA happens not to call is
  dropped).
- This spec covers **the fork only**. LISA's cutover (Drizzle schema and
  migration, call-site rewrites, moving live jobs from Mongo, the direct
  collection access sites) gets its own spec once this one is approved.

Success means: every existing Pulse behaviour works against Postgres, several
processes can poll one table concurrently without double-running a job, and
the fork has no `mongodb` dependency.

### What LISA uses today (drives the design)

- Constructed with a URL (`db.address`), collection `pulseJobs`,
  `processEvery: '3 seconds'`, `maxConcurrency: 100`,
  `defaultLockLifetime: 1h`; `resumeOnRestart` left at its default (on).
- Prod runs 3–6 worker replicas that all poll and lock; up to 10 web replicas,
  the MCP server and the public API only schedule.
- Called: `define`, `create` + `schedule` / `repeatEvery` / `priority` / `save`,
  `jobs(query, sort, limit, skip)`, `cancel`, `disable`, `job.remove`,
  `job.touch`, `start`, `stop`, and hand-edited `attrs` followed by `save()`.
- About 12 distinct Mongo filter shapes are passed in: `$exists`, `$lte`/`$gt`
  on dates, case-insensitive `$regex` on `name`, `$in`, `{ _id: ObjectId }`.

## Approach

All SQL lives in one module, `src/store/` (`PgJobStore`), together with the
single row ⇄ `JobAttributes` mapper. `Pulse` and `Job` methods call
`this._store.*` instead of `this._collection.*`. The processing loop in
`src/utils/process-jobs.ts` keeps its structure; only its two direct
collection calls move to the store.

Rejected:

- Inline SQL at each call site: smaller diff, but SQL and column mapping end
  up scattered across about 12 files.
- Drizzle inside the fork: couples the fork's Drizzle version to LISA's and
  pulls an ORM into a library that needs about ten statements.

## §1 Storage and schema

### Connection

```ts
new Pulse({ pg: { pool, table: 'pulse_jobs' }, ... })
```

- `pool` is a caller-owned `pg.Pool`. Pulse never creates, ends or otherwise
  owns it.
- `table` defaults to `pulse_jobs`. It is validated as a plain SQL identifier
  (`/^[a-z_][a-z0-9_]*$/i`, optionally `schema.table`) and quoted, because it is
  interpolated into every statement.
- Every store operation is a single statement on the pool in autocommit mode.
  No explicit transactions, no held clients.

### Schema ownership

- The fork exports `pulseJobsDdl(table: string): string`, the canonical
  `CREATE TABLE` and `CREATE INDEX` statements.
- `pulse.ensureSchema()` runs that DDL (`IF NOT EXISTS`). It is opt-in:
  config `autoSchema: true` runs it during start-up before `ready` is emitted.
  Default is `false`. This replaces `disableAutoIndex`.
- LISA mirrors the DDL as a Drizzle schema plus migration in `packages/db`, so
  the table follows LISA's normal migration review and rollout. The fork's own
  tests use `ensureSchema()`.

### Table

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid` PK, default `gen_random_uuid()` | exposed as `attrs._id` (string) |
| `name` | `text not null` | |
| `type` | `text not null` | `normal` / `single` / `once` |
| `priority` | `integer not null default 0` | |
| `disabled` | `boolean not null default false` | |
| `data` | `jsonb` | |
| `result` | `jsonb` | |
| `backoff` | `jsonb` | `{ type, delay }` |
| `next_run_at` | `timestamptz` | |
| `locked_at` | `timestamptz` | |
| `last_run_at` | `timestamptz` | |
| `last_finished_at` | `timestamptz` | |
| `failed_at` | `timestamptz` | |
| `start_date` | `timestamptz` | |
| `end_date` | `timestamptz` | |
| `repeat_interval` | `text` | |
| `repeat_timezone` | `text` | |
| `repeat_at` | `text` | |
| `skip_days` | `text` | |
| `fail_reason` | `text` | |
| `last_modified_by` | `text` | |
| `run_count` | `integer` | nullable: "never ran" stays distinct from 0 |
| `finished_count` | `integer` | nullable |
| `fail_count` | `integer` | nullable |
| `attempts` | `integer` | |
| `progress` | `real` | |
| `should_save_result` | `boolean not null default false` | |
| `unique_key` | `text` | canonical JSON of the `unique()` key |

### Indexes

| Index | Purpose |
|---|---|
| `(name, next_run_at, priority DESC) WHERE locked_at IS NULL AND NOT disabled` | lock query, default sort |
| `(locked_at) WHERE locked_at IS NOT NULL` | expired-lock branch, `resumeOnRestart`, stats |
| `UNIQUE (name) WHERE type = 'single'` | `every()` upsert |
| `UNIQUE (name, unique_key) WHERE unique_key IS NOT NULL` | `unique()` upsert |
| `(last_finished_at)` | retention and stats queries |
| `GIN (data jsonb_path_ops)` | `data` containment filter |

### Mapping rules

- `attrs._id` keeps its name and becomes a uuid `string` (was `ObjectId`).
- Every attribute is a column, so "unset" means `NULL`. An attribute that is
  `undefined` on save is written as `NULL`. LISA's admin retry and run-now
  (`lockedAt = undefined; save()`) depend on this.
- Integer counters and dates map `NULL` → `undefined` on read, so
  `attrs.runCount` is `undefined` for a job that never ran, as today.
- Dates inside `data` and `result` are stored as jsonb and come back as ISO
  strings, not `Date` objects. This is a documented breaking change; the fork
  does not try to revive them.

## §2 Locking and processing

Timestamps are generated on the worker (`new Date()`) and passed as
parameters, as today, so the in-memory checks in `process-jobs.ts`
(`lockedAt < lockDeadline`) agree with the database.

### Lock next job (`findAndLockNextJob` → `store.lockNext`)

```sql
UPDATE <table> SET locked_at = $now
WHERE id = (
  SELECT id FROM <table>
  WHERE name = $name AND NOT disabled
    AND ((locked_at IS NULL AND next_run_at <= $nextScanAt)
         OR locked_at <= $lockDeadline)
  ORDER BY <sort>            -- from config.sort, whitelisted columns
  LIMIT 1
  FOR UPDATE SKIP LOCKED)
RETURNING *;
```

Concurrent replicas skip rows another replica is locking right now: no
blocking and no double-locking. `lockLimit`, concurrency and queue handling in
`process-jobs.ts` are unchanged.

### Lock on the fly (`store.lockById`)

```sql
UPDATE <table> SET locked_at = $now
WHERE id = $id AND locked_at IS NULL AND next_run_at = $nextRunAt AND NOT disabled
RETURNING *;
```

### Save (`saveJob` → `store.save`)

All paths return `id, next_run_at`. The existing follow-up is kept: if the
returned `nextRunAt` is before `_nextScanAt`, process the job immediately.

1. **Job has `_id`:** full-row `UPDATE … WHERE id = $id RETURNING …`. Zero
   rows (the job was cancelled meanwhile) is a no-op that returns the job
   unchanged, as today.
2. **`type = 'single'`** (from `every()`):
   `INSERT … ON CONFLICT (name) WHERE type = 'single' DO UPDATE SET …`. If the
   incoming `nextRunAt` is at or before now, `next_run_at` is left out of the
   `DO UPDATE` list, so an existing schedule is not moved. This matches
   today's `$setOnInsert` protection.
3. **`unique_key` set** (from `unique()`):
   `INSERT … ON CONFLICT (name, unique_key) WHERE unique_key IS NOT NULL`
   `DO UPDATE SET …`. With `insertOnly`, `DO NOTHING`, then select the
   existing row by `(name, unique_key)` and return it.
4. **Otherwise:** plain `INSERT … RETURNING …`.

`lastModifiedBy` is still set to the Pulse instance name on every save.

### Stop (`store.unlock`)

`UPDATE <table> SET locked_at = NULL WHERE id = ANY($ids)`.

### Resume on restart (behaviour change)

Today the first branch releases any locked job whose `runCount` equals
`finishedCount` or whose `lastFinishedAt` is missing. With several replicas
this releases a first-ever run that is executing right now on another replica,
which then runs twice. The same bug exists on Mongo today.

New predicate:

```sql
UPDATE <table>
SET locked_at = NULL, last_modified_by = NULL, last_run_at = NULL, next_run_at = $now
WHERE (locked_at IS NOT NULL
       AND locked_at <= $now - lock_lifetime(name)
       AND next_run_at IS NOT NULL
       AND (run_count IS NOT DISTINCT FROM finished_count OR last_finished_at IS NULL))
   OR (locked_at IS NULL
       AND last_finished_at IS NULL
       AND next_run_at IS NOT NULL AND next_run_at <= $now);
```

`lock_lifetime(name)` is the definition's `lockLifetime`, falling back to
`defaultLockLifetime`. It is applied as a per-name `CASE` built from the
definitions known when the statement runs, with the default for unknown names.
Only expired locks are released; in-flight jobs on other replicas are left
alone.

The statement runs in `start()`, not when the store is attached (today it runs
from `dbInit`). By `start()` the definitions, and so their lock lifetimes, are
known. Instances that only schedule and never call `start()` (LISA's web, MCP
and public API processes) no longer touch locks at all. LISA's separate `stale-lock-cleanup.ts` becomes redundant (removed in
the LISA spec).

## §3 Public API changes

### Typed filter

Used by `jobs()`, `countJobs()`, `cancel()`, `disable()` and `enable()`. All
fields are optional and ANDed; everything compiles to parameterized SQL.

```ts
type DateCond = { isNull?: boolean; lt?: Date; lte?: Date; gt?: Date; gte?: Date };

interface JobFilter {
  ids?: string[];                    // id = ANY($)
  name?: string | string[];          // name = $ / name = ANY($)
  nameNotIn?: string[];              // used by purge()
  nameContains?: string;             // name ILIKE '%…%', % _ \ escaped
  status?: 'scheduled' | 'running' | 'completed' | 'failed';
  recurring?: boolean;               // repeat_interval IS [NOT] NULL
  disabled?: boolean;
  data?: Record<string, unknown>;    // data @> $::jsonb
  nextRunAt?: DateCond;
  lockedAt?: DateCond;
  lastFinishedAt?: DateCond;
  failedAt?: DateCond;
}
```

`status` is shorthand, copied from LISA's `scheduler.ts`:

| status | SQL |
|---|---|
| `running` | `locked_at IS NOT NULL` |
| `scheduled` | `locked_at IS NULL AND next_run_at IS NOT NULL` |
| `completed` | `last_finished_at IS NOT NULL AND failed_at IS NULL` |
| `failed` | `failed_at IS NOT NULL` |

An empty filter matches every row, as an empty Mongo query does today.

### Signatures

```ts
jobs(filter?: JobFilter, opts?: { sort?: JobSort; limit?: number; skip?: number }): Promise<Job[]>
countJobs(filter?: JobFilter): Promise<number>
cancel(filter: JobFilter): Promise<number>
disable(filter?: JobFilter): Promise<number>
enable(filter?: JobFilter): Promise<number>

type JobSort = Partial<Record<
  'nextRunAt' | 'lastRunAt' | 'lastFinishedAt' | 'failedAt' | 'priority' | 'name',
  'asc' | 'desc'
>>;
```

- `sort` keys go through a whitelist to column names; caller input never
  reaches the SQL text. The constructor's `sort` config uses `JobSort` too;
  default `{ nextRunAt: 'asc', priority: 'desc' }`.
- `limit` 0 or omitted means no limit, as today.
- `purge()` becomes `cancel({ nameNotIn: definedNames })`.
- `job.remove()` becomes `cancel({ ids: [this.attrs._id] })`.
- `job.fetchStatus()` reads by id through the store.
- `job.unique(key: Record<string, unknown>, opts?: { insertOnly?: boolean })`
  takes a plain object. `unique_key` is its canonical JSON (keys sorted
  recursively).

### Config

```ts
interface PulseConfig {
  name?: string;
  processEvery?: string;
  maxConcurrency?: number;
  defaultConcurrency?: number;
  lockLimit?: number;
  defaultLockLimit?: number;
  defaultLockLifetime?: number;
  sort?: JobSort;
  pg?: { pool: Pool; table?: string };
  autoSchema?: boolean;
  resumeOnRestart?: boolean;
}
```

`pulse.pg(pool, table?)` replaces `pulse.mongo(db, collection?)` for
connecting after construction. `ready` is emitted once the store is attached
(and, with `autoSchema`, the DDL has run). `error` is emitted if the DDL
fails.

### Removed

`database()`, `mongo()`, `dbInit()`, `hasMongoProtocol`, `getJobsRepo`,
`countJobsRepo`, `_db` / `_mdb` / `_collection`, `config.mongo`, `config.db`,
`disableAutoIndex`, and every `mongodb` type in the public types.

### Breaking changes summary (for the README)

1. Postgres only; connect with a `pg.Pool`.
2. `attrs._id` is a uuid string, not an `ObjectId`.
3. Filters are `JobFilter` objects, not Mongo queries; `jobs()` takes an
   options object instead of positional `sort, limit, skip`.
4. `unique()` takes a key object, not a Mongo filter.
5. Dates inside `data` / `result` come back as ISO strings.
6. `resumeOnRestart` only releases locks older than the lock lifetime, and
   runs in `start()` rather than on connect.

## §4 Testing and packaging

### Test database

`embedded-postgres` (devDependency, real Postgres binary, no Docker; LISA
already uses it). One server per test run; each test file gets its own schema
created through `ensureSchema()`.

### TDD order

1. Store unit tests against Postgres:
   - each statement (lockNext, lockById, all four save paths, unlock,
     resumeOnRestart, find/count/delete/update by filter);
   - the filter compiler: every field, `DateCond` combinations, `status`,
     `nameContains` escaping, sort whitelist, identifier validation;
   - the mapper: `NULL` ⇄ `undefined`, uuid, jsonb, counters.
2. Port `test/unit/pulse.spec.ts` from `mongodb-memory-server` to Postgres.
3. New behaviour tests:
   - concurrency: 3 Pulse instances on one table, N jobs, each job runs
     exactly once;
   - `every()` called in parallel from several instances leaves one row;
   - `resumeOnRestart` leaves another instance's live lock alone and releases
     an expired one.

### Packaging

- `pg ^8` as a peerDependency; `@types/pg` and `embedded-postgres` as
  devDependencies. Remove `mongodb` and `mongodb-memory-server`.
- Version `2.0.0`; update `description` and `keywords`.
- README / gitbook: a Postgres setup section, the DDL, and the breaking-change
  list above.
- `npm run build`, `npm run lint` and `npm test` must pass.

## Out of scope (LISA cutover spec)

- Drizzle schema and migration for `pulse_jobs` in `packages/db`.
- Rewriting call sites to `JobFilter` and string ids.
- Replacing direct collection access: stats (`scheduler.ts`), retention
  cleanup, `stale-lock-cleanup.ts`.
- Moving pending and recurring jobs from Mongo to Postgres at cutover.
- Sweeping job handlers for dates read from `job.attrs.data`.
- Dead code found during the survey (`scheduleGoogleDriveSyncJob`,
  `test:job-retry:js`).
