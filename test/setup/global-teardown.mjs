export default async function globalTeardown() {
  await globalThis.__PULSE_PG__?.stop();
}
