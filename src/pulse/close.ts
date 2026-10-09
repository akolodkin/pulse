import createDebugger from 'debug';
import { Pulse } from '.';

const debug = createDebugger('pulse:close');

export type CloseMethod = () => Promise<Pulse>;
/**
 * Kept for API compatibility. The pg Pool belongs to the caller, so there is
 * nothing for Pulse to close; end the pool yourself.
 * @name Pulse#close
 * @function
 */
export const close: CloseMethod = async function (this: Pulse) {
  debug('close called; the pg pool is owned by the caller and is left open');
  return this;
};
