import { Pulse } from '.';

export type EnsureSchemaMethod = () => Promise<void>;
/**
 * Create the jobs table and its indexes if they do not exist.
 * @name Pulse#ensureSchema
 * @function
 */
export const ensureSchema: EnsureSchemaMethod = async function (this: Pulse) {
  await this._store.ensureSchema();
};
