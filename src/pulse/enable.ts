import createDebugger from 'debug';
import { Pulse } from '.';
import { JobFilter } from '../store';

const debug = createDebugger('pulse:enable');

export type EnableMethod = (filter?: JobFilter) => Promise<number>;
/**
 * Enables any jobs matching `filter` by setting the `disabled` flag to `false`
 * @name Pulse#enable
 * @function
 * @param filter typed job filter
 * @returns {Promise<number>} Resolved with the number of jobs whose flag changed.
 */
export const enable: EnableMethod = async function (this: Pulse, filter = {}) {
  debug('attempting to enable all jobs matching filter', filter);
  const modifiedCount = await this._store.setDisabled(filter, false);
  debug('%s jobs enabled', modifiedCount);
  return modifiedCount;
};
