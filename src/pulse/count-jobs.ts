import { Pulse } from '.';
import { JobFilter } from '../store';

export type CountJobsMethod = (filter?: JobFilter) => Promise<number>;

/**
 * Counts all jobs matching `filter`
 * @name Pulse#countJobs
 * @function
 * @param [filter] typed job filter
 */
export const countJobs: CountJobsMethod = async function (this: Pulse, filter = {}) {
  return this._store.count(filter);
};
