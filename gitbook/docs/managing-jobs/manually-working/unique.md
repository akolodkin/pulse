# Unique



## `job.unique(key, options?)`

{% hint style="info" %}
The `unique` method configures a job to be unique according to a specified key object. This method is crucial for avoiding duplicate job entries in scenarios where job uniqueness is determined by specific data attributes.\
\
_This does **NOT** save the job in the database.  you must explicitly declare_ [_`save()`_](save.md)_if you want to save it_
{% endhint %}

### Example Usage

{% code fullWidth="false" %}
```typescript
const job = pulse.create('test', {});
job.unique({ type: 'email', userId: '12345' });
await job.save(); // If you want to save it

// Keep the existing job unchanged if one with the same key already exists
job.unique({ type: 'email', userId: '12345' }, { insertOnly: true });
```
{% endcode %}

### Parameters

* **`key`** (`Record<string, unknown>`): A plain object that identifies the job. Saving a job whose key equals an existing job's key (compared regardless of key order) updates that job instead of inserting a new one.
* **`options`** (`{ insertOnly: boolean }` - optional): Additional options to control the uniqueness check. The `insertOnly` option, if set to `true`, keeps the existing job unchanged when one with the same key already exists, instead of updating it.

### Returns

* **`Job`**: Returns the job instance with the uniqueness criteria applied. This allows for method chaining.

\




