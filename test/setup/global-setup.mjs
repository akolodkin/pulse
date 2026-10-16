import EmbeddedPostgres from 'embedded-postgres';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export default async function globalSetup() {
  const port = 54000 + Math.floor(Math.random() * 1000);
  const pg = new EmbeddedPostgres({
    databaseDir: mkdtempSync(join(tmpdir(), 'pulse-pg-')),
    user: 'postgres',
    password: 'postgres',
    port,
    persistent: false,
    onLog: () => {},
  });
  await pg.initialise();
  await pg.start();
  await pg.createDatabase('pulse_test');
  globalThis.__PULSE_PG__ = pg;
  process.env.PULSE_TEST_DATABASE_URL = `postgresql://postgres:postgres@127.0.0.1:${port}/pulse_test`;
}
