# Sort



## `pulse.sort(sort)`

{% hint style="info" %}
The `sort` method customizes the sorting order of jobs when querying the database. This order can significantly affect the efficiency and priority with which jobs are processed.
{% endhint %}

### Example Usage

```typescript
const pulse = new Pulse();

// Set a custom sort order for job processing
pulse.sort({ nextRunAt: 'desc' });

//or new Pulse({ sort: { nextRunAt: 'desc' } });
```



### Parameters

* **`sort`** (`JobSort`): An object mapping a field to `'asc'` or `'desc'`. Allowed fields: `nextRunAt`, `lastRunAt`, `lastFinishedAt`, `failedAt`, `priority`, `name`. For example, `{ nextRunAt: 'asc', priority: 'desc' }` will sort jobs primarily by their next scheduled run time in ascending order and by priority in descending order if there are ties.

### Returns

* **`Pulse`**: Returns the instance of the `Pulse` class, enabling method chaining.



