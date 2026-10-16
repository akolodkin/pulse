import createDebugger from 'debug';
import { Pulse } from '.';
import { Job } from '../job';
import { createJob } from '../utils';

const debug = createDebugger('pulse:internal:_findAndLockNextJob');

/**
 * Find and lock jobs
 * @name Pulse#findAndLockNextJob
 * @function
 * @param jobName name of job to try to lock
 * @param definition definition used to tell how job is run
 * @access protected
 * @caller jobQueueFilling() only
 */
export const findAndLockNextJob = async function (
  this: Pulse,
  jobName: string,
  definition: { lockLifetime: number }
): Promise<Job | undefined> {
  debug('_findAndLockNextJob(%s, [Function])', jobName);
  const record = await this._store.lockNext(jobName, {
    now: new Date(),
    nextScanAt: this._nextScanAt,
    lockDeadline: new Date(Date.now() - definition.lockLifetime),
    sort: this._sort,
  });

  if (!record) {
    return undefined;
  }
  debug('found a job available to lock, creating a new job on Pulse with id [%s]', record._id);
  return createJob(this, record);
};
