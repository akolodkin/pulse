import { canonicalJson } from '../../../src/store/canonical-json';

describe('canonicalJson', () => {
  test('ignores key order, recursively', () => {
    expect(canonicalJson({ b: 1, a: { d: 2, c: 3 } })).toBe(canonicalJson({ a: { c: 3, d: 2 }, b: 1 }));
    expect(canonicalJson({ b: 1, a: { d: 2, c: 3 } })).toBe('{"a":{"c":3,"d":2},"b":1}');
  });

  test('keeps array order', () => {
    expect(canonicalJson({ a: [2, 1] })).not.toBe(canonicalJson({ a: [1, 2] }));
  });

  test('serialises Dates as ISO strings', () => {
    expect(canonicalJson({ at: new Date('2026-01-01T00:00:00Z') })).toBe('{"at":"2026-01-01T00:00:00.000Z"}');
  });
});
