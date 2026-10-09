# Cancel



## `pulse.cancel(filter)`

{% hint style="info" %}
The `cancel` method is designed to cancel and permanently remove jobs from the database that match a specified filter. This functionality is critical for managing job lifecycles, particularly when certain jobs are no longer needed or conditions change requiring their termination.
{% endhint %}

### Example Usage

```typescript
const pulse = new Pulse();

// Cancel every job with a given name
pulse.cancel({ name: 'send email report' })
  .then(deletedCount => console.log(`${deletedCount} jobs cancelled`))
  .catch(error => console.error('Failed to cancel jobs:', error));

// Cancel specific jobs by id
pulse.cancel({ ids: [job.attrs._id] });
```



### Parameters

* **`filter`** (`JobFilter`): A typed filter that specifies which jobs to cancel and delete; see [`jobs`](jobs.md) for the fields. `{}` matches every job and cancels all of them. Unknown fields throw a `PulseError` instead of being ignored.

### Returns

* **`Promise<number>`**: Returns a promise that resolves with the number of jobs deleted from the database, `0` if none matched.

\




