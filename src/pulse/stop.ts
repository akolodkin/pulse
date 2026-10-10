import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:stop');

export type StopMethod = () => Promise<void>;
/**
 * Clear the interval that processes the jobs and release the locks of jobs this
 * instance locked but has not started. Running jobs keep their lock: if this
 * process finishes them, the completion save unlocks them; if it exits first,
 * another worker re-runs them once the lock lifetime expires. Unlocking a
 * running one-off job would strand it (it no longer has a nextRunAt).
 * @name Pulse#stop
 * @function
 * @returns resolves when the locks are released
 */
export const stop: StopMethod = async function (this: Pulse) {
  debug('Pulse.stop called, clearing interval for processJobs()');
  clearInterval(this._processInterval);
  this._processInterval = undefined;

  const notStarted = this._lockedJobs.filter((job) => !this._runningJobs.includes(job));
  const jobIds = notStarted.map((job) => job.attrs._id);
  if (this._store && jobIds.length > 0) {
    debug('about to unlock jobs with ids: %O', jobIds);
    await this._store.unlock(jobIds);
  }
  this._lockedJobs = this._lockedJobs.filter((job) => this._runningJobs.includes(job));
};
