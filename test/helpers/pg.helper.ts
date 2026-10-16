import { randomBytes } from 'crypto';
import { Pool } from 'pg';

export interface TestDb {
  pool: Pool;
  schema: string;
  table: string;
  cleanup: () => Promise<void>;
}

export const testDatabaseUrl = (): string => {
  const url = process.env.PULSE_TEST_DATABASE_URL;
  if (!url) {
    throw new Error('PULSE_TEST_DATABASE_URL is not set; run the tests through jest so globalSetup starts Postgres');
  }
  return url;
};

export async function createTestDb(): Promise<TestDb> {
  const pool = new Pool({ connectionString: testDatabaseUrl(), max: 10 });
  const schema = `t_${randomBytes(6).toString('hex')}`;
  await pool.query(`CREATE SCHEMA ${schema}`);

  return {
    pool,
    schema,
    table: `${schema}.pulse_jobs`,
    cleanup: async () => {
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
      await pool.end();
    },
  };
}
