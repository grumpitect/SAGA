const { MongoClient } = require('mongodb');
const SagaRunner = require('./SagaRunner');

module.exports = {
  async initalize({
    aliveTimeOut = 5, // seconds
    lockAcquisitionRetryTimeout = 100, // milliseconds
    name,
    sagaList,
    mongoUrl,
  }) {
    const client = await MongoClient.connect(mongoUrl);
    const db = client.db('synapse-saga');

    const runners = db.collection('runners');
    const queue = db.collection('queue');
    const logs = db.collection('logs');
    const locks = db.collection('locks');

    const runner = new SagaRunner({
      aliveTimeOut,
      lockAcquisitionRetryTimeout,
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
