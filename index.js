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

class SagaLogger {
  constructor() {
    this.logs = {};
  }

  create(sagaId) {
    // read from db and store it in cache
    this.logs[sagaId] = this.logs[sagaId] || [];

    return {
      read: () => {
        // read from cache
        return this.logs[sagaId];
      },
      log: (data) => {
        // add to db and cache and include log-date
        this.logs[sagaId].push(data);
        console.log(`${sagaId}`, data);
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
  constructor(id, sagaLogger) {
    this.id = id;
    this.sagaLogger = sagaLogger;
  }

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

  async execute(saga, initalParams) {
    const logger = await this.sagaLogger.create(saga.id);
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

const sec = new SagaExecutionCoordinator('SEC#1', new SagaLogger());

(async () => {
  const params = {
    x: 1,
    y: 2,
    z: 3,
  };
  const r1 = await sec.execute(saga1, params);
  console.log('r1', r1);
  const rAgain = await sec.execute(saga1, params);

  if ((r1.isRollback && !r1.isSuccess) || (r1.isTransaction && !r1.isSuccess)) {
    const r2 = await sec.execute(saga1, params);
    console.log('r2', r2);

    if (r2.isRollback && !r2.isSuccess) {
      const r3 = await sec.execute(saga1, params);
      console.log('r3', r3);
    }
  }

  console.log(rAgain, value);
})();
