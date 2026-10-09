import createDebugger from 'debug';
import type { Pool } from 'pg';
import { Pulse } from '.';
import { PgJobStore } from '../store';

const debug = createDebugger('pulse:pg');

export type PgMethod = (pool: Pool, table?: string) => Pulse;
/**
 * Attach a caller-owned pg Pool. Emits 'ready' asynchronously, after the
 * schema is created when `autoSchema` is on, so listeners attached right
 * after the constructor still fire.
 * @name Pulse#pg
 * @function
 * @param pool pg Pool; Pulse never ends it
 * @param [table] table name, `table` or `schema.table`. Defaults to `pulse_jobs`
 */
export const pg: PgMethod = function (this: Pulse, pool, table = 'pulse_jobs') {
  debug('attaching postgres store using table [%s]', table);
  this._store = new PgJobStore(pool, table);

  const schemaReady = this._autoSchema ? this._store.ensureSchema() : Promise.resolve();
  schemaReady
    .then(() => this.emit('ready'))
    .catch((error) => {
      debug('schema creation failed');
      this.emit('error', error);
    });

  return this;
};
