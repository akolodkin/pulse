import { Job } from '.';

interface FetchStatusResponse {
  status: boolean;
  reason?: string;
  lastRunAt?: Date;
  lockedAt?: Date;
  lastFinishedAt?: Date;
}

export type FetchStatusMethod = () => Promise<FetchStatusResponse>;

export const fetchStatus: FetchStatusMethod = async function (this: Job) {
  const [dbJob] = await this.pulse._store.find({ ids: [this.attrs._id] });
  if (!dbJob) {
    return {
      status: false,
      reason: `job with id ${this.attrs._id} not found in database`,
    };
  }

  this.attrs.lastRunAt = dbJob.lastRunAt;
  this.attrs.lockedAt = dbJob.lockedAt;
  this.attrs.lastFinishedAt = dbJob.lastFinishedAt;

  return {
    status: true,
    lastRunAt: this.attrs.lastRunAt,
    lockedAt: this.attrs.lockedAt ?? undefined,
    lastFinishedAt: this.attrs.lastFinishedAt,
  };
};
