import createDebugger from 'debug';
import { Pulse } from '.';
import { JobSort } from '../store';

const debug = createDebugger('pulse:sort');

export type SortMethod = (sort: JobSort) => Pulse;
/**
 * Set the sort used when picking the next job to lock
 * @name Pulse#sort
 * @function
 * @param sort e.g. { nextRunAt: 'asc', priority: 'desc' }
 */
export const sort: SortMethod = function (this: Pulse, sort) {
  debug('Pulse.sort(%O)', sort);
  this._sort = sort;
  return this;
};
