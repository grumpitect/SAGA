const _ = require('lodash');
const moment = require('moment');
const utils = require('./utils');
const SagaExecutionCoordinator = require('./SagaExecutionCoordinator');
const SagaLogger = require('./SagaLogger');

const QUEUE_STATE_RUNNING = 'running';
const QUEUE_STATE_ZOMBIE = 'zombie';

class SagaRunner {
  constructor({
    aliveTimeOut, // seconds
    lockAcquisitionRetryTimeout, // milliseconds
    name,
    sagaList,
    collections,
  }) {
    if (!collections) {
      throw new Error('collections parameter is required.');
    }

    if (!utils.isMongoCollection(collections.runners)) {
      throw new Error('collections parameter must have `runners` property of type `MongoDB Collection`');
    }

    if (!utils.isMongoCollection(collections.queue)) {
      throw new Error('collections parameter must have `queue` property of type `MongoDB Collection`');
    }

    if (!utils.isMongoCollection(collections.logs)) {
      throw new Error('collections parameter must have `logs` property of type `MongoDB Collection`');
    }

    if (!utils.isMongoCollection(collections.locks)) {
      throw new Error('collections parameter must have `locks` property of type `MongoDB Collection`');
    }

    if (!name || !_.isString(name)) {
      throw new Error('SagaQueue constructor needs parameter `name` of type `String`');
    }

    if (!sagaList || !_.isArray(sagaList)) {
      throw new Error('SagaQueue constructor needs parameter `sagaList` of type `Array`');
    }

    this.lockAcquisitionRetryTimeout = lockAcquisitionRetryTimeout;
    this.aliveTimeOut = aliveTimeOut;
    this.instanceId = new Date().getTime();
    this.name = name;
    this.sagaList = sagaList;
    this.collections = collections;

    this.sec = new SagaExecutionCoordinator();
    this.sagaLogger = new SagaLogger();
  }

  async initalize() {
    const {
      locks,
      queue,
    } = this.collections;

    await locks.createIndex({
      runner: 1,
      isPending: 1,
    }, {
      unique: true,
    });

    await locks.createIndex({
      isAcquired: 1,
    }, {
      unique: true,
      partialFilterExpression: {
        isAcquired: { $exists: true },
      },
    });

    await queue.createIndex({
      key: 1,
      sagaId: 1,
    }, {
      unique: true,
    });
  }

  async updateLastUpdate() {
    const {
      runners,
    } = this.collections;

    return runners.findOneAndUpdate({
      name: this.name,
    }, {
      $currentDate: {
        lastUpdate: true,
      },
      $set: {
        name: this.name,
      },
    }, {
      upsert: true,
    });
  }

  async acquireLock() {
    const {
      locks,
      runners,
    } = this.collections;

    const loop = utils.createConditionalLoop(async (next, finish) => {
      await this.updateLastUpdate();

      // add this runner as pending (waiting to acuire lock)
      // maybe we are already in the queue, in that case an exception would be thrown because of the index
      try {
        await locks.insertOne({
          runner: this.name,
          isPending: true,
          requestedDate: new Date(),
        });
      } catch (ex) {
        ex.toString(); // do nothing, we are already in the lock queue
      }

      const acquirer = await locks.findOne({
        isAcquired: true,
      });

      if (acquirer) {
        const runner = await runners.findOne({
          name: acquirer.runner,
        });

        const aliveTimeOut = moment().subtract(this.aliveTimeOut, 'seconds');
        const runnerLastUpdate = moment(runner.lastUpdate);

        if (
          runnerLastUpdate.isBefore(aliveTimeOut) ||
          (acquirer.runner === this.name && acquirer.instanceId !== this.instanceId)
        ) {
          // the runner is dead, so remove it from the lock
          // or it was another instance (dead) of this runner
          await locks.deleteOne({
            runner: acquirer.runner,
          });
        }

        return next();
      }

      // try to acuire lock
      let newAcquirer = null;
      try {
        newAcquirer = await locks.findOneAndUpdate({
          isPending: true,
        }, {
          $set: {
            isAcquired: true,
          },
        }, {
          sort: {
            requestedDate: 1,
          },
        });
      } catch (ex) {
        ex.toString();
      }

      if (newAcquirer && newAcquirer.value.runner === this.name) {
        await locks.findOneAndUpdate({
          runner: this.name,
        }, {
          $set: {
            instanceId: this.instanceId,
          },
        });

        return finish();
      }

      return next();
    }, this.lockAcquisitionRetryTimeout);

    return loop.start();
  }

  async releaseLock() {
    const {
      locks,
    } = this.collections;

    return locks.deleteOne({
      runner: this.name,
    });
  }

  async runWithinLock(callback) {
    await this.acquireLock();

    try {
      return await callback();
    } finally {
      await this.releaseLock();
    }
  }

  async markZombiesAndCaptureOne() {
    return this.runWithinLock(async () => {
      const {
        queue,
      } = this.collections;

      const aliveTimeOut = moment().subtract(this.aliveTimeOut, 'seconds').toDate();

      const orphanQueueItems = await queue.aggregate([
        {
          $lookup: {
            from: 'runners',
            localField: 'runner',
            foreignField: 'name',
            as: 'runner',
          },
        },
        {
          $match: {
            state: QUEUE_STATE_RUNNING,
            $or: [
              {
                runner: {
                  $elemMatch: {
                    lastUpdate: {
                      $lt: aliveTimeOut,
                    },
                  },
                },
              },
              {
                runner: {
                  $elemMatch: {
                    name: this.name,
                  },
                },
                instanceId: {
                  $ne: this.instanceId,
                },
              },
            ],
          },
        },
        {
          $limit: 10,
        },
        {
          $project: {
            _id: 1,
          },
        },
      ]).toArray();

      const zombieIds = orphanQueueItems.map(item => item._id);
      await queue.updateMany({
        _id: {
          $in: zombieIds,
        },
      }, {
        $set: {
          state: QUEUE_STATE_ZOMBIE,
        },
      });

      const capturedZombie = await queue.findOneAndUpdate({
        state: QUEUE_STATE_ZOMBIE,
      }, {
        $set: {
          state: QUEUE_STATE_RUNNING,
          runner: this.name,
          instanceId: this.instanceId,
        },
      });

      return capturedZombie && capturedZombie.value;
    });
  }

  async markAsZombie(queueItemId) {
    await this.runWithinLock(async () => {
      const {
        queue,
      } = this.collections;

      await queue.findOneAndUpdate({
        _id: queueItemId,
      }, {
        $set: {
          state: QUEUE_STATE_ZOMBIE,
        },
      });
    });
  }

  async unqueueSaga(queueItemId, logId) {
    await this.runWithinLock(async () => {
      const {
        queue,
        logs,
      } = this.collections;

      await Promise.all([
        queue.deleteOne({
          _id: queueItemId,
        }),
        logs.deleteOne({
          _id: logId,
        }),
      ]);
    });
  }

  async enqueue(sagaId, initalParams, key) {
    return this.runWithinLock(async () => {
      const {
        queue,
        logs,
      } = this.collections;

      const insertLogResult = await logs.insertOne({
        items: [],
      });
      const logId = insertLogResult.insertedId;

      // maybe we already have this saga and this key added to the queue
      // so the index would prevent re-adding it
      try {
        const insertQueueItemResult = await queue.insertOne({
          key,
          sagaId,
          initalParams,
          logId,
          runner: this.name,
          instanceId: this.instanceId,
          state: QUEUE_STATE_RUNNING,
        });

        return {
          _id: insertQueueItemResult.insertedId,
          logId,
        };
      } catch (ex) {
        ex.toString();

        await logs.deleteOne({
          _id: logId,
        });
      }

      return null;
    });
  }

  async execute(sagaId, initalParams, key) {
    const saga = _.find(this.sagaList, item => item.id === sagaId);

    utils.validateSaga(saga);

    if (!initalParams || !_.isPlainObject(initalParams)) {
      throw new Error('initalParams must be of type `PlainObject`');
    }

    if (!key || !_.isString(key)) {
      throw new Error('key must be of type `String`');
    }

    const queueItem = await this.enqueue(sagaId, initalParams, key);
    if (queueItem) {
      const {
        logs,
      } = this.collections;
      const logger = await this.sagaLogger.find(logs, queueItem.logId);

      return this.runSaga({
        saga,
        initalParams,
        logger,
        queueItemId: queueItem._id,
        logId: queueItem.logId,
      });
    }

    return {
      isFailed: true,
      duplicateKey: true,
    };
  }

  async runSaga({
    saga,
    initalParams,
    logger,
    queueItemId,
    logId,
  }) {
    const result = await this.sec.execute(
      saga,
      initalParams,
      logger,
    );

    if (result.isSuccess) {
      await this.unqueueSaga(queueItemId, logId);
    } else {
      await this.markAsZombie(queueItemId);
    }

    return result;
  }

  async cleanupLogs() {
    const {
      logs,
    } = this.collections;

    // find logs that are not in the queue
    const orphanLogs = await logs.aggregate([
      {
        $lookup: {
          from: 'queue',
          localField: '_id',
          foreignField: 'logId',
          as: 'queue',
        },
      },
      {
        $match: {
          queue: {
            $size: 0,
          },
        },
      },
      {
        $project: {
          _id: 1,
        },
      },
    ]).toArray();

    const orphanLogIds = orphanLogs.map(log => log._id);

    await logs.deleteMany({
      _id: {
        $in: orphanLogIds,
      },
    });
  }

  start() {
    const {
      logs,
    } = this.collections;

    const aliveLoop = utils.createEndlessLoop(async () => {
      await this.updateLastUpdate();
    }, 500);

    const cleanupLoop = utils.createEndlessLoop(async () => {
      await this.cleanupLogs();

      const zombie = await this.markZombiesAndCaptureOne();
      if (zombie) {
        const logger = await this.sagaLogger.find(logs, zombie.logId);
        const saga = _.find(this.sagaList, item => item.id === zombie.sagaId);

        await this.runSaga({
          saga,
          logger,
          initalParams: zombie.initalParams,
          queueItemId: zombie._id,
          logId: zombie.logId,
        });
      }
    }, 500);

    aliveLoop.start();
    cleanupLoop.start();
  }
}

module.exports = SagaRunner;
