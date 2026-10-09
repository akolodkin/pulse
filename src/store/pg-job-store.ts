import type { Pool } from 'pg';
import { pulseJobsDdl } from './ddl';
import { compileOrderBy, compileWhere, JobFilter, JobSort } from './filter';
import { ColumnValues, fromRow, JobRecord } from './row-mapper';
import { quoteTableName } from './table';

export interface SavedRef {
  _id: string;
  nextRunAt: Date | null;
}

export interface FindOptions {
  sort?: JobSort;
  limit?: number;
  skip?: number;
}

export interface LockNextOptions {
  now: Date;
  nextScanAt: Date;
  lockDeadline: Date;
  sort: JobSort;
}

const RETURNING_REF = 'RETURNING id, next_run_at';

const toSavedRef = (row: { id: string; next_run_at: Date | null }): SavedRef => ({
  _id: row.id,
  nextRunAt: row.next_run_at,
});

/** Every SQL statement Pulse runs. Each method is one statement on the caller's pool. */
export class PgJobStore {
  private readonly table: string;

  constructor(
    private readonly pool: Pool,
    private readonly tableName = 'pulse_jobs'
  ) {
    this.table = quoteTableName(tableName);
  }

  async ensureSchema(): Promise<void> {
    await this.pool.query(pulseJobsDdl(this.tableName));
  }

  async insert(values: ColumnValues): Promise<SavedRef> {
    const { columns, placeholders, params } = this.insertParts(values);
    const { rows } = await this.pool.query(
      `INSERT INTO ${this.table} (${columns}) VALUES (${placeholders}) ${RETURNING_REF}`,
      params
    );
    return toSavedRef(rows[0]);
  }

  async update(id: string, values: ColumnValues): Promise<SavedRef | undefined> {
    const params: unknown[] = [id];
    const assignments = values.map(([column, value]) => {
      params.push(value);
      return `${column} = $${params.length}`;
    });
    const { rows } = await this.pool.query(
      `UPDATE ${this.table} SET ${assignments.join(', ')} WHERE id = $1 ${RETURNING_REF}`,
      params
    );
    return rows[0] ? toSavedRef(rows[0]) : undefined;
  }

  async upsertSingle(values: ColumnValues, keepExistingNextRunAt: boolean): Promise<SavedRef> {
    const { columns, placeholders, params } = this.insertParts(values);
    const updates = values
      .map(([column]) => column)
      .filter((column) => !(keepExistingNextRunAt && column === 'next_run_at'))
      .map((column) => `${column} = EXCLUDED.${column}`);
    const { rows } = await this.pool.query(
      `INSERT INTO ${this.table} (${columns}) VALUES (${placeholders})
       ON CONFLICT (name) WHERE type = 'single' DO UPDATE SET ${updates.join(', ')}
       ${RETURNING_REF}`,
      params
    );
    return toSavedRef(rows[0]);
  }

  async upsertUnique(values: ColumnValues, insertOnly: boolean): Promise<SavedRef> {
    const { columns, placeholders, params } = this.insertParts(values);
    const conflict = `ON CONFLICT (name, unique_key) WHERE unique_key IS NOT NULL`;
    const action = insertOnly
      ? 'DO NOTHING'
      : `DO UPDATE SET ${values.map(([column]) => `${column} = EXCLUDED.${column}`).join(', ')}`;
    const { rows } = await this.pool.query(
      `INSERT INTO ${this.table} (${columns}) VALUES (${placeholders}) ${conflict} ${action} ${RETURNING_REF}`,
      params
    );
    if (rows[0]) {
      return toSavedRef(rows[0]);
    }

    // insertOnly hit an existing row: return that row, as an insert-only upsert does.
    const name = values.find(([column]) => column === 'name')?.[1];
    const uniqueKey = values.find(([column]) => column === 'unique_key')?.[1];
    const existing = await this.pool.query(
      `SELECT id, next_run_at FROM ${this.table} WHERE name = $1 AND unique_key = $2`,
      [name, uniqueKey]
    );
    return toSavedRef(existing.rows[0]);
  }

  async find(filter: JobFilter = {}, { sort = {}, limit = 0, skip = 0 }: FindOptions = {}): Promise<JobRecord[]> {
    const params: unknown[] = [];
    let sql = `SELECT * FROM ${this.table} WHERE ${compileWhere(filter, params)} ${compileOrderBy(sort)}`;
    if (limit > 0) {
      params.push(limit);
      sql += ` LIMIT $${params.length}`;
    }
    if (skip > 0) {
      params.push(skip);
      sql += ` OFFSET $${params.length}`;
    }
    const { rows } = await this.pool.query(sql, params);
    return rows.map(fromRow);
  }

  async count(filter: JobFilter = {}): Promise<number> {
    const params: unknown[] = [];
    const { rows } = await this.pool.query(
      `SELECT count(*)::int AS count FROM ${this.table} WHERE ${compileWhere(filter, params)}`,
      params
    );
    return rows[0].count;
  }

  async delete(filter: JobFilter): Promise<number> {
    const params: unknown[] = [];
    const result = await this.pool.query(`DELETE FROM ${this.table} WHERE ${compileWhere(filter, params)}`, params);
    return result.rowCount ?? 0;
  }

  async setDisabled(filter: JobFilter, disabled: boolean): Promise<number> {
    const params: unknown[] = [];
    const where = compileWhere(filter, params);
    params.push(disabled);
    const flag = `$${params.length}`;
    const result = await this.pool.query(
      `UPDATE ${this.table} SET disabled = ${flag} WHERE ${where} AND disabled IS DISTINCT FROM ${flag}`,
      params
    );
    return result.rowCount ?? 0;
  }

  async unlock(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }
    await this.pool.query(`UPDATE ${this.table} SET locked_at = NULL WHERE id = ANY($1::uuid[])`, [ids]);
  }

  /**
   * Locks the next due job of `name`. SKIP LOCKED lets concurrent pollers
   * pass over a row another poller is locking instead of waiting or taking it too.
   */
  async lockNext(
    name: string,
    { now, nextScanAt, lockDeadline, sort }: LockNextOptions
  ): Promise<JobRecord | undefined> {
    const { rows } = await this.pool.query(
      `UPDATE ${this.table} SET locked_at = $1
       WHERE id = (
         SELECT id FROM ${this.table}
         WHERE name = $2 AND NOT disabled
           AND ((locked_at IS NULL AND next_run_at <= $3) OR locked_at <= $4)
         ${compileOrderBy(sort)}
         LIMIT 1
         FOR UPDATE SKIP LOCKED)
       RETURNING *`,
      [now, name, nextScanAt, lockDeadline]
    );
    return rows[0] ? fromRow(rows[0]) : undefined;
  }

  async lockById(id: string, nextRunAt: Date | null | undefined, now: Date): Promise<JobRecord | undefined> {
    const { rows } = await this.pool.query(
      `UPDATE ${this.table} SET locked_at = $1
       WHERE id = $2 AND locked_at IS NULL AND next_run_at IS NOT DISTINCT FROM $3 AND NOT disabled
       RETURNING *`,
      [now, id, nextRunAt ?? null]
    );
    return rows[0] ? fromRow(rows[0]) : undefined;
  }

  /**
   * Makes interrupted work runnable again. Only locks older than the job's lock
   * lifetime are released, so jobs running on other processes are left alone.
   */
  async resumeUnfinished(now: Date, lockDeadlines: Record<string, Date>, defaultLockDeadline: Date): Promise<number> {
    const names = Object.keys(lockDeadlines);
    const result = await this.pool.query(
      `WITH deadline(name, at) AS (SELECT * FROM unnest($2::text[], $3::timestamptz[]))
       UPDATE ${this.table} AS job
       SET locked_at = NULL, last_modified_by = NULL, last_run_at = NULL, next_run_at = $1
       WHERE (job.locked_at IS NOT NULL
              AND job.locked_at <= COALESCE((SELECT at FROM deadline WHERE deadline.name = job.name), $4)
              AND job.next_run_at IS NOT NULL
              AND (job.run_count IS NOT DISTINCT FROM job.finished_count OR job.last_finished_at IS NULL))
          OR (job.locked_at IS NULL
              AND job.last_finished_at IS NULL
              AND job.next_run_at <= $1)`,
      [now, names, names.map((name) => lockDeadlines[name]), defaultLockDeadline]
    );
    return result.rowCount ?? 0;
  }

  private insertParts(values: ColumnValues) {
    return {
      columns: values.map(([column]) => column).join(', '),
      placeholders: values.map((_, index) => `$${index + 1}`).join(', '),
      params: values.map(([, value]) => value),
    };
  }
}
