import createDebugger from 'debug';
import { Pulse } from '.';
import { JobFilter } from '../store';

const debug = createDebugger('pulse:cancel');

export type CancelMethod = (filter: JobFilter) => Promise<number>;
/**
 * Cancels any jobs matching `filter` and removes them from the database.
 * @name Pulse#cancel
 * @function
 * @param filter typed job filter
 * @caller client code, Pulse.purge(), Job.remove()
 */
export const cancel: CancelMethod = async function (this: Pulse, filter) {
  debug('attempting to cancel all Pulse jobs', filter);
  const deletedCount = await this._store.delete(filter);
  this.emit('cancel', deletedCount);
  debug('%s jobs cancelled', deletedCount);
  return deletedCount;
};
