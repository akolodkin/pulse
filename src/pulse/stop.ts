import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:stop');

export type StopMethod = () => Promise<void>;
/**
 * Clear the interval that processes the jobs and release this instance's locks
 * @name Pulse#stop
 * @function
 * @returns resolves when the locks are released
 */
export const stop: StopMethod = async function (this: Pulse) {
  debug('Pulse.stop called, clearing interval for processJobs()');
  clearInterval(this._processInterval);
  this._processInterval = undefined;

  const jobIds = this._lockedJobs.map((job) => job.attrs._id);
  if (this._store && jobIds.length > 0) {
    debug('about to unlock jobs with ids: %O', jobIds);
    await this._store.unlock(jobIds);
  }
  this._lockedJobs = [];
};
