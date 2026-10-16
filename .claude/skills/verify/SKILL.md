---
name: verify
description: Verify a Pulse change at its real surface — the packed npm package used by separate Node worker processes against a real Postgres.
---

# Verify Pulse (Postgres)

Surface = the published package, used from a separate app, with several OS processes on one table (that is how LISA runs it: 3–6 workers + schedule-only processes).

## Setup

```bash
SP=$(mktemp -d)            # or the session scratchpad
npm pack --pack-destination $SP          # runs `prepare` → build
mkdir $SP/app && cd $SP/app && npm init -y >/dev/null
npm install ../pulsecron-pulse-*.tgz pg@^8
docker run -d --rm --name pulse-verify-pg -e POSTGRES_PASSWORD=pw -p 127.0.0.1:55432:5432 postgres:16
until docker exec pulse-verify-pg pg_isready -U postgres; do sleep 1; done
```

Connection string: `postgresql://postgres:pw@127.0.0.1:55432/postgres`. Create the table with `autoSchema: true` (safe from several processes at once — it takes an advisory lock).

## Flows worth driving (each as its own `node` process)

- Schedule N jobs from one process, run 3 worker processes (`processEvery: '1 second'`) that log `RUN <id> pid`; assert N runs, 0 duplicate ids, nothing left locked.
- Live lock: worker A runs a 30s job (lockLifetime 10 min); boot worker B mid-run → B must not run it.
- Crash: `kill -9` a worker mid-job with a short lockLifetime; a new worker picks it up after the lock expires.
- 3 processes call `every('3 seconds', 'tick')` at once → 1 row; tick worker reschedules each run.
- Misuse through the public API: Mongo-style filters (`{ _id }`, `{ 'data.x' }`, `{ nextRunAt: Date }`, `$lte`), `cancel({ name: undefined })`, bad sort, bad table name, malformed ids → must throw / match nothing, row count unchanged.

## Gotchas

- `npm run build` after deleting `dist/` emits nothing while `tsconfig.tsbuildinfo` exists (incremental) — delete both.
- A 30s job with a 4s lockLifetime is re-run by the same worker (no `touch()`): expected lock semantics, not a bug.
- Clean up: `docker rm -f pulse-verify-pg`.
