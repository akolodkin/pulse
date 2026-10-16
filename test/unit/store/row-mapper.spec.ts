import { JobAttributes } from '../../../src/job';
import { fromRow, toColumnValues } from '../../../src/store/row-mapper';

const attrs = (value: Record<string, unknown>) => value as unknown as Partial<JobAttributes>;

describe('toColumnValues', () => {
  test('maps present attributes to snake_case columns', () => {
    const nextRunAt = new Date('2026-01-01T00:00:00Z');
    expect(toColumnValues(attrs({ name: 'a', type: 'normal', nextRunAt, runCount: 2 }))).toEqual([
      ['name', 'a'],
      ['type', 'normal'],
      ['next_run_at', nextRunAt],
      ['run_count', 2],
    ]);
  });

  test('skips absent attributes so updates leave those columns alone', () => {
    expect(toColumnValues(attrs({ name: 'a' }))).toEqual([['name', 'a']]);
  });

  test('writes present-but-undefined and null attributes as NULL', () => {
    expect(toColumnValues(attrs({ lockedAt: undefined, failedAt: null }))).toEqual([
      ['locked_at', null],
      ['failed_at', null],
    ]);
  });

  test('treats undefined as absent for NOT NULL columns with a default', () => {
    expect(toColumnValues(attrs({ priority: undefined, disabled: undefined, shouldSaveResult: undefined }))).toEqual(
      []
    );
  });

  test('serialises json columns, so Dates inside data become ISO strings', () => {
    const when = new Date('2026-02-03T04:05:06.000Z');
    expect(toColumnValues(attrs({ data: { when, list: [1, 2] }, backoff: { type: 'fixed', delay: 5 } }))).toEqual([
      ['data', '{"when":"2026-02-03T04:05:06.000Z","list":[1,2]}'],
      ['backoff', '{"type":"fixed","delay":5}'],
    ]);
  });

  test('converts numeric and string dates to Date', () => {
    const [[, start], [, end]] = toColumnValues(attrs({ startDate: 0, endDate: '2026-01-01T00:00:00Z' }));
    expect(start).toEqual(new Date(0));
    expect(end).toEqual(new Date('2026-01-01T00:00:00Z'));
  });

  test('does not persist attributes without a column', () => {
    expect(
      toColumnValues(attrs({ _id: 'x', uniqueKey: { a: 1 }, uniqueOpts: { insertOnly: true }, extra: 1 }))
    ).toEqual([]);
  });
});

describe('fromRow', () => {
  test('maps columns back to attributes and id to _id', () => {
    const nextRunAt = new Date();
    expect(
      fromRow({ id: 'abc', name: 'a', type: 'normal', priority: 0, next_run_at: nextRunAt, data: { x: 1 } })
    ).toEqual({ _id: 'abc', name: 'a', type: 'normal', priority: 0, nextRunAt, data: { x: 1 } });
  });

  test('leaves NULL columns out, so counters read as undefined', () => {
    const record = fromRow({ id: 'abc', name: 'a', run_count: null, locked_at: null, unique_key: 'k' });
    expect(record).toEqual({ _id: 'abc', name: 'a' });
    expect('runCount' in record).toBe(false);
  });
});
