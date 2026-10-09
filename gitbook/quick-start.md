# Quick Start

### Installation

```bash
npm i @pulsecron/pulse pg
```



### Example&#x20;

```typescript
import { Pool } from 'pg';
import Pulse from '@pulsecron/pulse';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const pulse = new Pulse({ pg: { pool }, autoSchema: true });

// Override the default table name (`pulse_jobs`, or `schema.table`):
// const pulse = new Pulse({ pg: { pool, table: 'my_schema.jobs' } });

// or attach the pool after construction:
// pulse.pg(pool, 'pulse_jobs');

/**
 * Example of defining a job
 */
pulse.define('delete old users', async (job) => {
  console.log('Deleting old users...');
  return;
}, { shouldSaveResult: true, attempts: 4, backoff: { type: 'exponential', delay: 1000 } });

/**
 * Example of repeating a job
 */
(async function () {
  // IIFE to give access to async/await
  await pulse.start();

  await pulse.every('3 minutes', 'delete old users');

  // Alternatively, you could also do:
  await pulse.every('*/3 * * * *', 'delete old users');
})();

/**
 * Example of defining a job with options
 */
pulse.define(
  'send email report',
  async (job) => {
    const { to } = job.attrs.data;

    console.log(`Sending email report to ${to}`);
  },
  { lockLifetime: 5 * 1000, priority: 'high', concurrency: 10 }
);

/**
 * Example of scheduling a job
 */
(async function () {
  await pulse.start();
  await pulse.schedule('in 20 minutes', 'send email report', { to: 'admin@example.com' });
})();

/**
 * Example of repeating a job
 */
(async function () {
  const weeklyReport = pulse.create('send email report', { to: 'example@example.com' });
  await pulse.start();
  await weeklyReport.repeatEvery('1 week').save();
})();

/**
 * Check job start and completion/failure
 */
pulse.on('start', (job) => {
  console.log(time(), `Job <${job.attrs.name}> starting`);
});
pulse.on('success', (job) => {
  console.log(time(), `Job <${job.attrs.name}> succeeded`);
});
pulse.on('fail', (error, job) => {
  console.log(time(), `Job <${job.attrs.name}> failed:`, error);
});

function time() {
  return new Date().toTimeString().split(' ')[0];
}
```
