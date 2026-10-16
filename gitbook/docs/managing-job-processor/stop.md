# Stop



## `pulse.stop()`

{% hint style="info" %}
The `stop` method halts the job processing activity initiated by the `start` method, effectively stopping the job processing interval. It also releases the locks of jobs this instance had locked but not yet started, so other workers can pick them up straight away. Jobs that are already running keep their lock: if the process lets them finish, they are unlocked as usual; if the process exits first, another worker re-runs them once their `lockLifetime` expires, so a job is never lost on shutdown. Give long jobs a `lockLifetime` that covers their run time (or call `job.touch()`).
{% endhint %}

### Example Usage

```typescript
const pulse = new Pulse();
pulse.start();

async function graceful() {
  await pulse.stop();
  process.exit(0);
}

process.on('SIGTERM', graceful);
process.on('SIGINT' , graceful);
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  graceful();
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception thrown', error);
  graceful();
});

```



### Parameters

*

### Returns

* **`Promise<void>`**: A promise that resolves when the job processing interval is cleared and all locked jobs are unlocked. It handles any failures during the unlocking process.

\




