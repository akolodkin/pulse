# Jobs



## `pulse.jobs(filter?, options?)`

{% hint style="info" %}
The `jobs` method queries the database to retrieve an array of jobs that match a specified filter. It supports sorting, limiting the number of results, and skipping a specified number of records, making it a versatile tool for job management and monitoring.
{% endhint %}

### Example Usage

```typescript
const pulse = new Pulse();

// Fetch the first 10 completed jobs, skipping the first 5, sorted by most recently finished
pulse.jobs({ status: 'completed' }, { sort: { lastFinishedAt: 'desc' }, limit: 10, skip: 5 })
  .then(jobs => {
    console.log('Retrieved completed jobs:', jobs);
  })
  .catch(error => {
    console.error('Error fetching jobs:', error);
  });
```



### Parameters

* **`filter`** (`JobFilter` - optional): A typed filter selecting which jobs to retrieve. All fields are optional and combined with AND. If omitted (or `{}`), every job matches. Unknown fields throw a `PulseError`.
  * **`ids`** (`string[]`): job ids. Entries that are not valid uuids match nothing.
  * **`name`** (`string | string[]`): exact job name, or any of several names.
  * **`nameNotIn`** (`string[]`): jobs whose name is none of these.
  * **`nameContains`** (`string`): case-insensitive substring of the name.
  * **`status`** (`'scheduled' | 'running' | 'completed' | 'failed'`): `running` = locked; `scheduled` = unlocked with a next run; `completed` = finished without a failure; `failed` = has a failure time.
  * **`recurring`** (`boolean`): `true` for jobs with a repeat interval, `false` for one-off jobs.
  * **`disabled`** (`boolean`): the `disabled` flag.
  * **`data`** (`Record<string, unknown>`): jobs whose `data` contains this object (JSON containment).
  * **`nextRunAt`, `lockedAt`, `lastFinishedAt`, `failedAt`** (`DateCond`): `{ isNull?, lt?, lte?, gt?, gte? }` conditions on that date.
* **`options`** (`{ sort?, limit?, skip? }` - optional):
  * **`sort`** (`JobSort`): order of the results, e.g. `{ nextRunAt: 'asc', priority: 'desc' }`. Allowed fields: `nextRunAt`, `lastRunAt`, `lastFinishedAt`, `failedAt`, `priority`, `name`; directions are `'asc'` and `'desc'`.
  * **`limit`** (`number`): The maximum number of jobs to return. If not specified, or if specified as `0`, no limit will be applied.
  * **`skip`** (`number`): The number of jobs to skip before starting to return the results. This can be used for pagination.

### Returns

* **`Promise<Job[]>`**: A promise that resolves to an array of `Job` objects that match the query and adhere to the specified sorting and pagination settings.

\




