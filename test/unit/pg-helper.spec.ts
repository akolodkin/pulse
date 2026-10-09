import { createTestDb } from '../helpers/pg.helper';

describe('createTestDb', () => {
  test('gives each caller its own schema and drops it on cleanup', async () => {
    const db = await createTestDb();
    const other = await createTestDb();

    expect(db.schema).not.toEqual(other.schema);
    expect(db.table).toEqual(`${db.schema}.pulse_jobs`);

    const exists = async (schema: string) =>
      (await other.pool.query('SELECT 1 FROM pg_namespace WHERE nspname = $1', [schema])).rowCount;

    expect(await exists(db.schema)).toBe(1);
    await db.cleanup();
    expect(await exists(db.schema)).toBe(0);
    await other.cleanup();
  });
});
