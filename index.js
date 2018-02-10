const { MongoClient } = require('mongodb');
const SagaRunner = require('./SagaRunner');

module.exports = {
  async initalize({
    aliveLoopTimeout = 500, // milliseconds
    cleanUpLoopTimeout = 500, // milliseconds
    lockHoldTimeout = 1000, // milliseconds
    lockAcquisitionRetryTimeout = 100, // milliseconds
    keepLogsFor = 6 * 31, // days
    name,
    sagaList,
    mongoUrl,
    mongoDBName,
  }) {
    const client = await MongoClient.connect(mongoUrl);
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
      name,
      sagaList,
      collections: {
        runners,
        queue,
        logs,
        locks,
      },
    });

    await runner.initalize();

    return runner;
  },
};
