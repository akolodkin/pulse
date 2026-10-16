const sortKeys = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value && typeof value === 'object' && !(value instanceof Date)) {
    const source = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.keys(source)
        .sort()
        .map((key) => [key, sortKeys(source[key])])
    );
  }
  return value;
};

/** JSON with object keys sorted at every level, so equal keys compare equal as text. */
export const canonicalJson = (value: unknown): string => JSON.stringify(sortKeys(value));
