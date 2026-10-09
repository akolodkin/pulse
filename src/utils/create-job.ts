import { Job } from '../job';
import { Pulse } from '../pulse';
import { JobRecord } from '../store';

/**
 * Create Job object from a stored record
 * @param {Object} pulse instance of Pulse
 * @param {Object} record job attributes read from the database
 * @returns {Job} returns created job
 */
export const createJob = (pulse: Pulse, record: JobRecord): Job => new Job({ ...record, pulse });
