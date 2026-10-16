import { EventEmitter } from 'events';
import humanInterval from 'human-interval';
import type { Pool } from 'pg';
import { Job } from '../job';
import { CancelMethod, cancel } from './cancel';
import { CloseMethod, close } from './close';
import { CountJobsMethod, countJobs } from './count-jobs';
import { CreateMethod, create } from './create';
import { DefaultConcurrencyMethod, defaultConcurrency } from './default-concurrency';
import { DefaultLockLifetimeMethod, defaultLockLifetime } from './default-lock-lifetime';
import { DefaultLockLimitMethod, defaultLockLimit } from './default-lock-limit';
import { DefineMethod, define } from './define';
import { DisableMethod, disable } from './disable';
import { DrainMethod, drain } from './drain';
import { EnableMethod, enable } from './enable';
import { EnsureSchemaMethod, ensureSchema } from './ensure-schema';
import { EveryMethod, every } from './every';
import { findAndLockNextJob } from './find-and-lock-next-job';
import { JobProcessingQueue } from './job-processing-queue';
import { JobsMethod, jobs } from './jobs';
import { LockLimitMethod, lockLimit } from './lock-limit';
import { MaxConcurrencyMethod, maxConcurrency } from './max-concurrency';
import { NameMethod, name } from './name';
import { NowMethod, now } from './now';
import { PgMethod, pg } from './pg';
import { ProcessEveryMethod, processEvery } from './process-every';
import { PurgeMethod, purge } from './purge';
import { ResumeOnRestartMethod, resumeOnRestart } from './resume-on-restart';
import { resumeUnfinishedJobs } from './resume-unfinished-jobs';
import { SaveJobMethod, saveJob } from './save-job';
import { ScheduleMethod, schedule } from './schedule';
import { SortMethod, sort } from './sort';
import { StartMethod, start } from './start';
import { StopMethod, stop } from './stop';
import { compileOrderBy, JobSort, PgJobStore } from '../store';

export type PulseOnEventType = 'ready' | 'start' | 'success' | 'fail' | 'complete' | 'cancel' | 'error';
export interface PulseConfig {
  name?: string;
  processEvery?: string;
  maxConcurrency?: number;
  defaultConcurrency?: number;
  lockLimit?: number;
  defaultLockLimit?: number;
  defaultLockLifetime?: number;
  sort?: JobSort;
  pg?: {
    pool: Pool;
    table?: string;
  };
  autoSchema?: boolean;
  resumeOnRestart?: boolean;
}

/**
 * @class Pulse
 * @param {Object} config - Pulse Config
 * @property {Object} _name - Name of the current Pulse queue
 * @property {Number} _processEvery
 * @property {Number} _defaultConcurrency
 * @property {Number} _maxConcurrency
 * @property {Number} _defaultLockLimit
 * @property {Number} _lockLimit
 * @property {Object} _definitions
 * @property {Object} _runningJobs
 * @property {Object} _lockedJobs
 * @property {Object} _jobQueue
 * @property {Number} _defaultLockLifetime
 * @property {Object} _sort
 * @property {Object} _indices
 * @property {Boolean} _isLockingOnTheFly - true if 'lockingOnTheFly' is currently running. Prevent concurrent execution of this method.
 * @property {Map} _isJobQueueFilling - A map of jobQueues and if the 'jobQueueFilling' method is currently running for a given map. 'lockingOnTheFly' and 'jobQueueFilling' should not run concurrently for the same jobQueue. It can cause that lock limits aren't honored.
 * @property {Array} _jobsToLock
 */
class Pulse extends EventEmitter {
  private _lazyBindings: Record<string, any> = {};
  _defaultConcurrency: any;
  _defaultLockLifetime: any;
  _defaultLockLimit: any;
  _definitions: any;
  _findAndLockNextJob = findAndLockNextJob;
  _resumeOnRestart: boolean;
  _isLockingOnTheFly: boolean;
  _isJobQueueFilling: Map<string, boolean>;
  _jobQueue: JobProcessingQueue;
  _jobsToLock: Job[];
  _lockedJobs: Job[];
  _runningJobs: Job[];
  _lockLimit: any;
  _maxConcurrency: any;
  _name: any;
  _processEvery: number;
  _ready: Promise<unknown>;
  _sort: JobSort;
  _resumeUnfinishedJobs = resumeUnfinishedJobs;
  _autoSchema: boolean;
  _store!: PgJobStore;
  _nextScanAt: any;
  _processInterval: any;
  _readyAt: Date;

  /**
   * Constructs a new Pulse object.
   * @param config Optional configuration to initialize the Pulse.
   */
  constructor(config: PulseConfig = {}) {
    super();

    this._name = config.name;
    this._processEvery = (humanInterval(config.processEvery) ?? humanInterval('5 seconds')) as number; // eslint-disable-line @typescript-eslint/non-nullable-type-assertion-style
    this._defaultConcurrency = config.defaultConcurrency || 5;
    this._maxConcurrency = config.maxConcurrency || 20;
    this._defaultLockLimit = config.defaultLockLimit || 0;
    this._lockLimit = config.lockLimit || 0;
    this._definitions = {};
    this._runningJobs = [];
    this._lockedJobs = [];
    this._jobQueue = new JobProcessingQueue();
    this._defaultLockLifetime = config.defaultLockLifetime || 10 * 60 * 1000; // 10 minute default lockLifetime
    if (config.sort) {
      compileOrderBy(config.sort);
    }
    this._sort = config.sort || { nextRunAt: 'asc', priority: 'desc' };
    this._autoSchema = config.autoSchema === true;
    this._resumeOnRestart = config.resumeOnRestart !== false;

    this._isLockingOnTheFly = false;
    this._isJobQueueFilling = new Map<string, boolean>();
    this._jobsToLock = [];
    this._ready = new Promise((resolve) => {
      this.once('ready', resolve);
    });
    this._readyAt = new Date();

    this.init(config);
  }

  /**
   ***************************************
   * Public methods
   * *************************************
   */

  get define(): DefineMethod {
    return this.bindMethod('define', define);
  }

  get every(): EveryMethod {
    return this.bindMethod('every', every);
  }

  get processEvery(): ProcessEveryMethod {
    return this.bindMethod('processEvery', processEvery);
  }

  get cancel(): CancelMethod {
    return this.bindMethod('cancel', cancel);
  }

  get close(): CloseMethod {
    return this.bindMethod('close', close);
  }

  get create(): CreateMethod {
    return this.bindMethod('create', create);
  }

  get defaultConcurrency(): DefaultConcurrencyMethod {
    return this.bindMethod('defaultConcurrency', defaultConcurrency);
  }

  get defaultLockLifetime(): DefaultLockLifetimeMethod {
    return this.bindMethod('defaultLockLifetime', defaultLockLifetime);
  }

  get defaultLockLimit(): DefaultLockLimitMethod {
    return this.bindMethod('defaultLockLimit', defaultLockLimit);
  }

  get disable(): DisableMethod {
    return this.bindMethod('disable', disable);
  }

  get enable(): EnableMethod {
    return this.bindMethod('enable', enable);
  }

  get jobs(): JobsMethod {
    return this.bindMethod('jobs', jobs);
  }

  get countJobs(): CountJobsMethod {
    return this.bindMethod('countJobs', countJobs);
  }

  get lockLimit(): LockLimitMethod {
    return this.bindMethod('lockLimit', lockLimit);
  }

  get maxConcurrency(): MaxConcurrencyMethod {
    return this.bindMethod('maxConcurrency', maxConcurrency);
  }

  get name(): NameMethod {
    return this.bindMethod('name', name);
  }

  get now(): NowMethod {
    return this.bindMethod('now', now);
  }

  get purge(): PurgeMethod {
    return this.bindMethod('purge', purge);
  }

  get saveJob(): SaveJobMethod {
    return this.bindMethod('saveJob', saveJob);
  }

  get schedule(): ScheduleMethod {
    return this.bindMethod('schedule', schedule);
  }

  get sort(): SortMethod {
    return this.bindMethod('sort', sort);
  }

  get start(): StartMethod {
    return this.bindMethod('start', start);
  }

  get stop(): StopMethod {
    return this.bindMethod('stop', stop);
  }

  get drain(): DrainMethod {
    return this.bindMethod('drain', drain);
  }

  get pg(): PgMethod {
    return this.bindMethod('pg', pg);
  }

  get ensureSchema(): EnsureSchemaMethod {
    return this.bindMethod('ensureSchema', ensureSchema);
  }

  get resumeOnRestart(): ResumeOnRestartMethod {
    return this.bindMethod('resumeOnRestart', resumeOnRestart);
  }

  /**
   *
   ***************************************
   * Overridden methods
   * *************************************
   */

  on(event: PulseOnEventType, listener: (...arg: any[]) => void): this {
    return super.on(event, listener);
  }

  /**
   ***************************************
   * Private methods
   * *************************************
   */

  private init(config: PulseConfig) {
    if (config.pg) {
      this.pg(config.pg.pool, config.pg.table);
    }
  }

  private bindMethod<T extends Function>(methodName: string, fn: T): T {
    if (!this._lazyBindings[methodName]) {
      this._lazyBindings[methodName] = fn.bind(this);
    }
    return this._lazyBindings[methodName] as T;
  }
}

export { Pulse };
