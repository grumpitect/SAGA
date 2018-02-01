const _ = require('lodash');
const moment = require('moment');
const utils = require('./utils');

let value = 0;

const t1 = async (params) => {
  value += params.inital.x;

  return 44;
};

let c = 0;
const c1 = async (params) => {
  // if (c++ === 2) {
  //   throw new Error('failed on c1');
  // }
  value -= params.inital.x;

  return -44;
};

const t2 = async (params) => {
  // throw new Error('failed on t2');
  value += params.inital.y;

  return 33;
};

const c2 = async (params) => {
  // if (c++ === 0) {
  //   throw new Error('failed on c2');
  // }
  value -= params.inital.y;

  return -33;
};

const t3 = async (params) => {
  // throw new Error('failed on t3');
  value += params.inital.z;

  return 22;
};

const c3 = async (params) => {
  value -= params.inital.z;

  return -22;
};

const saga1 = {
  id: 'saga#1',
  flow: [
    {
      id: 'step1',
      transaction: t1,
      compensation: c1,
    },
    {
      id: 'step2',
      transaction: t2,
      compensation: c2,
    },
    {
      id: 'step3',
      transaction: t3,
      compensation: c3,
    },
  ],
};

// todo: implement this using Mongo
class SagaLogger {
  constructor() {
    this.logs = {};
  }

  find(logId) {

  }

  create() {
    // read from db and store it in cache
    this.logs[newId] = this.logs[newId] || [];

    return {
      read: () => {
        // read from cache
        return this.logs[newId];
      },
      log: (data) => {
        // add to db and cache and include log-date
        this.logs[newId].push(data);
      },
    };
  }
}

const BEGIN_SAGA_TRANSACTIONS = {
  transaction: true,
  isBegin: true,
};

const END_SAGA_TRANSACTIONS = {
  transaction: true,
  isEnd: true,
};

const BEGIN_SAGA_COMPENSATIONS = {
  compensation: true,
  isBegin: true,
};

const END_SAGA_COMPENSATIONS = {
  compensation: true,
  isEnd: true,
};

const STAGE_PENDING = 'STAGE_PENDING';
const STAGE_COMPLETED = 'STAGE_COMPLETED';

class SagaExecutionCoordinator {
  async prepareParams(saga, initalParams, logs) {
    const flow = {};

    for (const log of logs) {
      if ((log.step || log.step === 0) && (log.stage === STAGE_COMPLETED || log.isError)) {
        const tcId = saga.flow[log.step].id;
        flow[tcId] = flow[tcId] || {
          transaction: {},
          compensation: {},
        };

        let prop = 'compensation';
        if (log.transaction) {
          prop = 'transaction';
        }

        if (log.isStep && log.stage === STAGE_COMPLETED) {
          flow[tcId][prop] = {
            value: log.result,
          };
          if (log.transaction) {
            delete flow.transactionError;
          } else {
            delete flow.compensationError;
          }
        } else if (log.isError) {
          flow[tcId][prop] = {
            isError: true,
            error: log.error,
            step: tcId,
          };

          if (log.transaction) {
            flow.transactionError = flow[tcId][prop];
          } else {
            flow.compensationError = flow[tcId][prop];
          }
        }
      }
    }

    const params = Object.assign({}, {
      inital: initalParams,
      flow,
    });

    return params;
  }

  async analyze(saga, logs) {
    let hasEnd = false;
    let hasBegin = false;
    let shouldRollback = false;
    let lastStage = null;
    let stageCompleted = true;
    let currentStep = -1;

    const logsLength = logs.length;
    for (let i = 0; i < logsLength; i += 1) {
      const log = logs[i];

      if (log.isStep) {
        if (log.stage === STAGE_PENDING) {
          stageCompleted = false;
        } else if (lastStage === STAGE_PENDING && log.stage === STAGE_COMPLETED) {
          stageCompleted = true;
        }

        lastStage = log.stage;
        currentStep = log.step;
      } else if (log.isError) {
        shouldRollback = true;
      } else if (log.isBegin) {
        hasBegin = true;
      } else if (log.isEnd) {
        hasEnd = true;
      }
    }

    if (!stageCompleted) {
      shouldRollback = true;
    }

    // always rollback if transaction flow is not completed
    // todo: this is here because I'm too lazy to fix the code above
    shouldRollback = hasBegin && !hasEnd;

    return {
      hasBegin,
      hasEnd,
      shouldRollback,
      fromStep: shouldRollback ? currentStep : currentStep + 1,
    };
  }

  async execute(saga, initalParams, logger) {
    const logs = await logger.read();
    const transactionLogs = logs.filter(log => log.transaction);
    const compensationLogs = logs.filter(log => log.compensation);

    const {
      hasBegin: transactionHasBegin,
      hasEnd: transactionHasEnd,
      fromStep: transactionFromStep,
      shouldRollback: transactionShouldRollback,
    } = await this.analyze(saga, transactionLogs);

    let result = null;

    if (transactionShouldRollback) {
      const {
        hasBegin: rollbackHasBegin,
        hasEnd: rollbackHasEnd,
        hasPending: rollbackHasPending,
        fromStep: rollbackFromStep,
      } = await this.analyze(saga, compensationLogs);

      result = await this.rollback({
        saga,
        initalParams,
        logger,
        fromStep: rollbackHasBegin ? rollbackFromStep : transactionFromStep,
        hasBegin: rollbackHasBegin,
        hasEnd: rollbackHasEnd,
        hasPending: rollbackHasPending,
      });
    } else {
      result = await this.continue({
        saga,
        initalParams,
        logger,
        fromStep: transactionFromStep,
        hasBegin: transactionHasBegin,
        hasEnd: transactionHasEnd,
      });
    }

    return Object.assign(result, {
      params: await this.prepareParams(saga, initalParams, await logger.read()),
    });
  }

  async continue({
    saga,
    initalParams,
    logger,
    fromStep,
    hasBegin,
    hasEnd,
  }) {
    if (!hasBegin) {
      await logger.log(BEGIN_SAGA_TRANSACTIONS);
    }

    const flowLength = saga.flow.length;
    for (let step = fromStep; step < flowLength; step += 1) {
      const {
        transaction,
      } = saga.flow[step];

      try {
        await logger.log({
          transaction: true,
          isStep: true,
          stage: STAGE_PENDING,
          step,
        });

        const params = await this.prepareParams(saga, initalParams, await logger.read());
        const result = await transaction(params);

        await logger.log({
          transaction: true,
          isStep: true,
          stage: STAGE_COMPLETED,
          step,
          result,
        });
      } catch (error) {
        await logger.log({
          transaction: true,
          isError: true,
          error,
          step,
        });

        return this.rollback({
          saga,
          initalParams,
          logger,
          fromStep: step - 1,
        });
      }
    }

    if (!hasEnd) {
      await logger.log(END_SAGA_TRANSACTIONS);
    }

    return {
      isTransaction: true,
      isSuccess: true,
    };
  }

  async rollback({
    saga,
    initalParams,
    logger,
    fromStep,
    hasBegin,
    hasEnd,
    hasPending,
  }) {
    if (!hasBegin) {
      await logger.log(BEGIN_SAGA_COMPENSATIONS);
    }

    for (let step = fromStep; step >= 0; step -= 1) {
      const {
        compensation,
      } = saga.flow[step];

      try {
        if (!hasPending) {
          await logger.log({
            compensation: true,
            isStep: true,
            stage: STAGE_PENDING,
            step,
          });
        }

        const params = await this.prepareParams(saga, initalParams, await logger.read());
        const result = await compensation(params);

        await logger.log({
          compensation: true,
          isStep: true,
          stage: STAGE_COMPLETED,
          step,
          result,
        });
      } catch (error) {
        await logger.log({
          compensation: true,
          isError: true,
          error,
          step,
        });

        return {
          isRollback: true,
          isSuccess: false,
        };
      }
    }

    if (!hasEnd) {
      await logger.log(END_SAGA_COMPENSATIONS);
    }

    return {
      isRollback: true,
      isSuccess: true,
    };
  }
}

const ALIVE_TIME_OUT = 5; // seconds
const LOCK_ACQUISITION_RETRY_TIMEOUT = 100; // milliseconds
const QUEUE_STATE_RUNNING = 'running';
const QUEUE_STATE_ZOMBIE = 'zombie';

class SagaRunner {
  constructor({
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

    return runners.findAndModify({
      name: this.name,
    }, [], {
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

        const aliveTimeOut = moment().subtract(ALIVE_TIME_OUT, 'seconds');
        const runnerLastUpdate = moment(runner.lastUpdate);

        if (runnerLastUpdate.isBefore(aliveTimeOut)) { // the runner is dead, so remove it from the lock
          await locks.deleteOne({
            runner: acquirer.runner,
          });
        }

        return next();
      }

      // try to acuire lock
      let newAcquirer = null;
      try {
        newAcquirer = await locks.findAndModify({
          isPending: true,
        }, [['requestedDate', 'asc']], {
          $set: {
            isAcquired: true,
          },
        }, {
          new: true,
        });
      } catch (ex) {
        ex.toString();
      }

      if (newAcquirer && newAcquirer.value.runner === this.name) {
        return finish();
      }

      return next();
    }, LOCK_ACQUISITION_RETRY_TIMEOUT);

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
      return callback();
    } finally {
      await this.releaseLock();
    }
  }

  async markZombiesAndGetOne() {
    await this.markAsRunning(zombie.sagaId);

    try {
      return this.runWithinLock(async () => {

      });
    } catch (ex) {
      console.log(ex);
    }

    return null;
  }

  async markAsZombie(sagaId) {

  }

  async unqueueSaga(sagaId) {
    this.runWithinLock(async () => {

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
        await queue.insertOne({
          key,
          sagaId,
          initalParams,
          logId,
          runner: this.name,
          state: QUEUE_STATE_RUNNING,
        });

        return {
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

    const queueItem = await this.enqueue(sagaId, initalParams, key);

    if (queueItem) {
      const logger = await this.sagaLogger.find(queueItem.logId);
      const result = await this.sec.execute(
        saga,
        initalParams,
        logger,
      );

      if (result.isSuccess) {
        await this.unqueueSaga(sagaId);
      } else {
        await this.markAsZombie(sagaId);
      }

      return result;
    }

    return {
      isFailed: true,
      duplicateKey: true,
    };
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
    const aliveLoop = utils.createEndlessLoop(async () => {
      await this.updateLastUpdate();
    }, 500);

    const cleanupLoop = utils.createEndlessLoop(async () => {
      await this.cleanupLogs();

      const zombie = await this.markZombiesAndGetOne();

      if (zombie) {
        const logger = await this.sagaLogger.find(zombie.logId);
        const saga = _.find(this.sagaList, item => item.id === zombie.sagaId);

        const result = await this.sec.execute(
          saga,
          zombie.sagaInitalParams,
          logger,
        );

        if (result.isSuccess) {
          await this.unqueueSaga(zombie.sagaId);
        } else {
          await this.markAsZombie(zombie.sagaId);
        }
      }
    }, 500);

    aliveLoop.start();
    cleanupLoop.start();
  }
}

const { MongoClient } = require('mongodb');

MongoClient.connect('mongodb://localhost:27017', (err, client) => {
  const db = client.db('synapse-saga');
  const runners = db.collection('runners');
  const queue = db.collection('queue');
  const logs = db.collection('logs');
  const locks = db.collection('locks');

  (async () => {
    const sagaList = [saga1];

    const runner1 = new SagaRunner({
      name: 'runner1',
      sagaList,
      collections: {
        runners,
        queue,
        logs,
        locks,
      },
    });

    await runner1.initalize();
    await runner1.execute(saga1.id, {
      x: 1,
      y: 2,
    }, 'myKey');

    // runner1.start();

    // const runner2 = new SagaRunner('runner2');
    // runner2.start();

    // await runner1.execute(saga1, 'myKey');
  })();
});

// (async () => {
//   const params = {
//     x: 1,
//     y: 2,
//     z: 3,
//   };
//   const r1 = await sec.execute(saga1, params);
//   console.log('r1', r1);
//   const rAgain = await sec.execute(saga1, params);

//   if ((r1.isRollback && !r1.isSuccess) || (r1.isTransaction && !r1.isSuccess)) {
//     const r2 = await sec.execute(saga1, params);
//     console.log('r2', r2);

//     if (r2.isRollback && !r2.isSuccess) {
//       const r3 = await sec.execute(saga1, params);
//       console.log('r3', r3);
//     }
//   }

//   console.log(rAgain, value);
// })();
