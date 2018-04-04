const _ = require('lodash');
const utils = require('./utils');
const FinishSaga = require('./ValueTypes/FinishSaga');

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
  async prepareParams(saga, initialParams, logs) {
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

    let mergedTransactionValues = {};
    let mergedCompensationValues = {};

    for (const tcId in flow) {
      const tc = flow[tcId];
      if (tc.transaction && tc.transaction.value) {
        mergedTransactionValues = _.merge(mergedTransactionValues, tc.transaction.value);
      } else if (tc.compensation && tc.compensation.value) {
        mergedCompensationValues = _.merge(mergedCompensationValues, tc.compensation.value);
      }
    }

    const params = Object.assign({}, {
      flow,
      initial: initialParams,
      transactionValues: mergedTransactionValues,
      compensationValues: mergedCompensationValues,
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
    const pendingStepCounts = {};

    const logsLength = logs.length;
    for (let i = 0; i < logsLength; i += 1) {
      const log = logs[i];

      if (log.isStep) {
        if (log.stage === STAGE_PENDING) {
          stageCompleted = false;
          pendingStepCounts[log.step] = pendingStepCounts[log.step] || {
            log,
            count: 0,
          };

          pendingStepCounts[log.step].count += 1;
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
      pendingStepCounts,
      maxPendingStepCount: _.maxBy(pendingStepCounts, (psc) => psc.count) || { count: 0 },
      fromStep: shouldRollback ? currentStep : currentStep + 1,
    };
  }
  // todo: implement this like async.auto dependency mechanism

  // todo: what happens when you make a request and it's timed out and the you compensate for it, and on the
  // other system it's get done!

  // todo: implmenet synapse in microservices (even for payment)

  // todo: when you want to compensate you have to first run the transaction successfully and then
  // use the data to compensate the transaction
  async execute({
    saga,
    initialParams,
    logger,
    rollbackRetryWarningThreshold,
    rollbackWaitTimeout,
    onTooManyRollbackAttempts,
  }) {
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
        maxPendingStepCount,
      } = await this.analyze(saga, compensationLogs);

      if (maxPendingStepCount.count >= rollbackRetryWarningThreshold) {
        if (onTooManyRollbackAttempts) {
          onTooManyRollbackAttempts(saga, logs);
        }
      }

      await utils.wait(rollbackWaitTimeout * Math.min(1, maxPendingStepCount.count));

      result = await this.rollback({
        saga,
        initialParams,
        logger,
        fromStep: rollbackHasBegin ? rollbackFromStep : transactionFromStep,
        hasBegin: rollbackHasBegin,
        hasEnd: rollbackHasEnd,
        hasPending: rollbackHasPending,
      });
    } else {
      result = await this.continue({
        saga,
        initialParams,
        logger,
        fromStep: transactionFromStep,
        hasBegin: transactionHasBegin,
        hasEnd: transactionHasEnd,
      });
    }

    return Object.assign(result, {
      params: await this.prepareParams(saga, initialParams, await logger.read()),
    });
  }

  async continue({
    saga,
    initialParams,
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

        const params = await this.prepareParams(saga, initialParams, await logger.read());
        const result = await transaction(params);

        await logger.log({
          transaction: true,
          isStep: true,
          stage: STAGE_COMPLETED,
          step,
          result,
        });

        if (result instanceof FinishSaga) {
          break;
        }
      } catch (error) {
        await logger.log({
          transaction: true,
          isError: true,
          error,
          step,
        });

        return this.rollback({
          saga,
          initialParams,
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
    initialParams,
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

        const params = await this.prepareParams(saga, initialParams, await logger.read());
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

module.exports = SagaExecutionCoordinator;
