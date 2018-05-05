const { MongoClient } = require('mongodb');
const SagaRunner = require('./SagaRunner');

const TransactionValue = require('./ValueTypes/TransactionValue');
const FinishSaga = require('./ValueTypes/FinishSaga');

module.exports = {
  valueTypes: {
    TransactionValue,
    FinishSaga,
  },
  async initialize({
    aliveLoopTimeout = 500, // milliseconds
    cleanUpLoopTimeout = 500, // milliseconds
    lockHoldTimeout = 1000, // milliseconds
    lockAcquisitionRetryTimeout = 100, // milliseconds
    keepLogsFor = 6 * 31, // days
    waitInsteadOfStopDuration = 1000, // milliseconds
    rollbackRetryWarningThreshold = 2, // count
    rollbackWaitTimeout = 1000, // milliseconds
    onTooManyRollbackAttempts, // callback
    name,
    sagaList,
    mongoUrl,
    mongoOptions,
    mongoDBName,
  }) {
    const client = await MongoClient.connect(mongoUrl, mongoOptions);
    const db = client.db(mongoDBName);

    const runners = db.collection('runners');
    const queue = db.collection('queue');
    const logs = db.collection('logs');
    const locks = db.collection('locks');

    const runner = new SagaRunner({
      aliveLoopTimeout,
      cleanUpLoopTimeout,
      lockHoldTimeout,
      lockAcquisitionRetryTimeout,
      keepLogsFor,
      waitInsteadOfStopDuration,
      rollbackRetryWarningThreshold,
      rollbackWaitTimeout,
      onTooManyRollbackAttempts,
      name,
      sagaList,
      collections: {
        runners,
        queue,
        logs,
        locks,
      },
    });

    await runner.initialize();

    return runner;
  },
};
