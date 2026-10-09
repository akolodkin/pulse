# Disable



## `pulse.disable(filter?)`

{% hint style="info" %}
The `disable` method updates job records in the database to set a `disabled` flag to `true`, effectively halting their execution by the job processor. This method allows for selective disabling of jobs without removing them from the database, providing a way to pause job execution as needed.
{% endhint %}

### Example Usage

```typescript
const pulse = new Pulse();

// Example of disabling every job named 'send email report'
pulse.disable({ name: 'send email report' })
  .then(modifiedCount => console.log(`${modifiedCount} jobs were disabled`))
  .catch(error => console.error('Failed to disable jobs:', error));
```



### Parameters

* **`filter`** (`JobFilter` - optional): A typed filter selecting the jobs to disable; see [`jobs`](jobs.md) for the fields. If omitted, it defaults to `{}`, which matches every job, so use it with caution. Unknown fields throw a `PulseError`.

### Returns

* **`Promise<number>`**: A promise that resolves with the number of jobs whose `disabled` flag changed. This count provides feedback on the impact of the call.

\




