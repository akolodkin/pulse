import { Job } from '.';

export type UniqueMethod = (key: Record<string, unknown>, options?: { insertOnly?: boolean }) => Job;
/**
 * Upsert on `key` instead of always inserting: a later save with an equal key
 * (compared as key-sorted JSON) updates that job, or keeps it with `insertOnly`.
 * @name Job#unique
 * @function
 * @param key identity of the job, e.g. { patientId }
 * @param options { insertOnly: true } keeps the existing job unchanged
 */
export const unique: UniqueMethod = function (this: Job, key, options?) {
  this.attrs.uniqueKey = key;
  this.attrs.uniqueOpts = options;
  return this;
};
