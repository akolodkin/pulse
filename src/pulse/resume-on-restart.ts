import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:resumeOnRestart');

export type ResumeOnRestartMethod = (resume?: boolean) => Pulse;

/**
 * Set the resume on restart flag. When on, start() releases expired locks and
 * reschedules past-due jobs that never ran before processing begins.
 * @name Pulse#resumeOnRestart
 * @function
 * @param resume resume on restart
 */
export const resumeOnRestart: ResumeOnRestartMethod = function (this: Pulse, resume = true) {
  debug('Pulse.resumeOnRestart(%s)', resume);
  this._resumeOnRestart = resume;
  return this;
};
