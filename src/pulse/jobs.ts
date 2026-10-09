import { Pulse } from '.';
import { Job } from '../job';
import { FindOptions, JobFilter } from '../store';
import { createJob } from '../utils';

export type JobsMethod = (filter?: JobFilter, options?: FindOptions) => Promise<Job[]>;

/**
 * Finds all jobs matching `filter`
 * @name Pulse#jobs
 * @function
 * @param [filter] typed job filter
 * @param [options] sort, limit and skip
 */
export const jobs: JobsMethod = async function (this: Pulse, filter = {}, options = {}) {
  const records = await this._store.find(filter, options);
  return records.map((record) => createJob(this, record));
};
