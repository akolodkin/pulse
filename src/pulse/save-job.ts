import createDebugger from 'debug';
import { Pulse } from '.';
import { Job } from '../job';
import { canonicalJson, SavedRef, toColumnValues } from '../store';
import { JobError, processJobs } from '../utils';

const debug = createDebugger('pulse:saveJob');

/**
 * Store the saved id and nextRunAt on the job, and process it immediately if it
 * would have been picked up by a scan that already happened.
 * @access private
 */
const processDbResult = async function (this: Pulse, job: Job, saved: SavedRef | undefined): Promise<Job> {
  if (!saved) {
    debug('[%s:%s] job no longer exists, nothing was saved', job.attrs.name, job.attrs._id);
    return job;
  }

  job.attrs._id = saved._id;
  job.attrs.nextRunAt = saved.nextRunAt;

  if (job.attrs.nextRunAt && job.attrs.nextRunAt < this._nextScanAt) {
    debug('[%s:%s] job would have ran by nextScanAt, processing the job immediately', job.attrs.name, saved._id);
    await processJobs.call(this, job);
  }

  return job;
};

export type SaveJobMethod = (job: Job) => Promise<Job>;
/**
 * Save the properties on a job to the database. Only attributes present on the
 * job are written, so columns it does not carry keep their stored values.
 * @name Pulse#saveJob
 * @function
 * @param job job to save
 * @returns resolves when job is saved or errors
 */
export const saveJob: SaveJobMethod = async function (this: Pulse, job) {
  if (!this._store) {
    throw new JobError('A db must be set up before you can save a job');
  }

  const id = job.attrs._id;
  const { uniqueKey, uniqueOpts } = job.attrs;

  const props = job.toJSON();
  props.lastModifiedBy = this._name;
  const values = toColumnValues(props);
  debug('[job %s] saving columns %O', id, values);

  if (id) {
    return processDbResult.call(this, job, await this._store.update(id, values));
  }

  if (props.type === 'single') {
    // A past nextRunAt must not move an existing 'single' job's schedule.
    const keepExistingNextRunAt = Boolean(props.nextRunAt && props.nextRunAt <= new Date());
    return processDbResult.call(this, job, await this._store.upsertSingle(values, keepExistingNextRunAt));
  }

  if (uniqueKey) {
    values.push(['unique_key', canonicalJson(uniqueKey)]);
    return processDbResult.call(this, job, await this._store.upsertUnique(values, Boolean(uniqueOpts?.insertOnly)));
  }

  return processDbResult.call(this, job, await this._store.insert(values));
};
