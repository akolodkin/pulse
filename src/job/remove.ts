import { Job } from '.';

export type RemoveMethod = () => Promise<number | undefined>;
/**
 * Remove the job from the database
 * @name Job#remove
 * @function
 */
export const remove: RemoveMethod = async function (this: Job) {
  return this.pulse.cancel({ ids: [this.attrs._id] });
};
