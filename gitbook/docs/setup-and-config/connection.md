# Connection

Pulse stores jobs in PostgreSQL (13+, the schema uses `gen_random_uuid()`). Pass it a `pg` Pool you own; Pulse never ends it.

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

With several processes, create the schema from one place (your migrations or a single `ensureSchema()` call), not with `autoSchema: true` on every replica.

With `autoSchema: true`, attach the `error` listener before the schema is created: a DDL failure is emitted as `error` and `ready` will not fire.

### `new Pulse(config?)`

#### Parameters

* **`config`**
  * **`name`** (`string` - optional): Specifies the name of the job queue. This can be used for identifying and managing different queues within the same application.
  * **`processEvery`** (`string` - optional): Defines how often the job processor should poll for new jobs to process. The string should be a human-readable interval, such as `'5 minutes'`, `'1 hour'`. Defaults to `'5 seconds'` if not specified.
  * **`maxConcurrency`** (`number` - optional): The maximum number of jobs that can be processed concurrently. Helps in controlling resource utilization. Defaults to `'20'` if not specified.
  * **`defaultConcurrency`** (`number` - optional): The default concurrency for jobs that do not specify their own concurrency setting. Defaults to `'5'` if not specified.
  * **`lockLimit`** (`number` - optional): Maximum number of jobs that can be locked at the same time. This prevents a single worker from locking too many jobs. Defaults to `'0'` if not specified.
  * **`defaultLockLimit`** (`number` - optional): Default limit for the number of jobs each worker can lock simultaneously. Defaults to `'0'` if not specified.
  * **`defaultLockLifetime`** (`number` - optional): Duration in milliseconds for how long a job can be locked before it is automatically unlocked. Useful for handling job crashes or stalls. Defaults to 600000 ms (10 minutes). Defaults to `'10 minutes'` if not specified.
  * **`sort`** (`JobSort` - optional): Determines the order in which jobs are selected and locked from the database. For example, `{ nextRunAt: 'asc', priority: 'desc' }` sorts by `nextRunAt` ascending and `priority` descending.
  * **`pg`** (`object`): The PostgreSQL connection. Includes:
    * **`pool`** (`pg.Pool`): A `pg` Pool that you own. Pulse never ends it.
    * **`table`** (`string` - optional): The table to use, as `table` or `schema.table` (letters, digits, underscores). Defaults to `pulse_jobs`.
  * **`autoSchema`** (`boolean` - optional): If `true`, Pulse creates the table and its indexes when the store is attached. Defaults to `false`.
  * **`resumeOnRestart`**(`boolean` - optional) - This config is used to ensure that jobs left unfinished due to an unexpected system shutdown or restart are properly resumed once the system is back online. Defaults to `true` if not specified.

## Upgrading from 1.x (MongoDB)

1. Connect with `config.pg` / `pulse.pg()`; `config.db`, `config.mongo`, `database()` and `mongo()` are gone. `close()` no longer closes anything.
2. `job.attrs._id` is a uuid string, not an `ObjectId`.
3. `jobs()`, `countJobs()`, `cancel()`, `disable()` and `enable()` take a `JobFilter`, not a Mongo query. `jobs()` takes `{ sort, limit, skip }` instead of positional arguments. Unknown filter fields throw.
4. `job.unique(key)` takes a plain key object; equal keys (in any key order) upsert the same job.
5. Dates inside `job.attrs.data` and `result` are stored as JSON and come back as ISO strings.
6. `resumeOnRestart` runs inside `start()` and only releases locks older than the job's `lockLifetime`.
7. `disableAutoIndex` is replaced by `autoSchema` (default `false`).
8. Config `sort` / `pulse.sort()` take `{ field: 'asc' | 'desc' }` (was Mongo `{ field: 1 | -1 }`).
