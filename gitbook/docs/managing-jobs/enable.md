# Enable



## `pulse.enable(filter?)`

{% hint style="info" %}
The `enable` method reverses the effects of the `disable` method by updating job records in the database to clear the `disabled` flag, allowing them to be processed again by the job scheduler. This is useful for resuming job execution after a temporary pause.
{% endhint %}

### Example Usage

```typescript
const pulse = new Pulse();

// Example of enabling every disabled job named 'send email report'
pulse.enable({ name: 'send email report', disabled: true })
  .then(modifiedCount => console.log(`${modifiedCount} jobs re-enabled after maintenance`))
  .catch(error => console.error('Failed to enable jobs:', error));
```



### Parameters

* **`filter`** (`JobFilter` - optional): A typed filter selecting the jobs to enable; see [`jobs`](jobs.md) for the fields. If omitted, it defaults to `{}`, which matches every job, so use it with caution. Unknown fields throw a `PulseError`.

### Returns

* **`Promise<number>`**: A promise that resolves with the number of jobs whose `disabled` flag changed. This count provides an indication of how many jobs were affected by the operation.

\




