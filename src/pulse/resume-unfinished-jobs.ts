import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:resumeUnfinishedJobs');

/**
 * Release locks older than each job's lock lifetime and reschedule
 * never-run jobs that are past due. Live locks held by other processes are
 * left alone. Called by start() when resumeOnRestart is on.
 * @access protected
 */
export const resumeUnfinishedJobs = async function (this: Pulse): Promise<number> {
  const now = new Date();
  const lockDeadlines: Record<string, Date> = {};
  for (const [name, definition] of Object.entries<{ lockLifetime: number }>(this._definitions)) {
    lockDeadlines[name] = new Date(now.getTime() - definition.lockLifetime);
  }

  const resumed = await this._store.resumeUnfinished(
    now,
    lockDeadlines,
    new Date(now.getTime() - this._defaultLockLifetime)
  );
  if (resumed > 0) {
    debug('resumed %d unfinished jobs (%s)', resumed, now.toISOString());
  }
  return resumed;
};
