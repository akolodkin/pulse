import createDebugger from 'debug';
import { Pulse } from '.';
import { JobFilter } from '../store';

const debug = createDebugger('pulse:disable');

export type DisableMethod = (filter?: JobFilter) => Promise<number>;
/**
 * Disables any jobs matching `filter` by setting the `disabled` flag to `true`
 * @name Pulse#disable
 * @function
 * @param filter typed job filter
 * @returns {Promise<number>} Resolved with the number of jobs whose flag changed.
 */
export const disable: DisableMethod = async function (this: Pulse, filter = {}) {
  debug('attempting to disable all jobs matching filter', filter);
  const modifiedCount = await this._store.setDisabled(filter, true);
  debug('%s jobs disabled', modifiedCount);
  return modifiedCount;
};
