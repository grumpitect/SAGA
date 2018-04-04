const TransactionValue = require('./ValueTypes/TransactionValue');
const FinishSaga = require('./ValueTypes/FinishSaga');

class SagaLogger {
  async create(logsCollection) {
    const insertLogResult = await logsCollection.insertOne({
      createdDate: new Date(),
      items: [],
    });

    return insertLogResult.insertedId;
  }

  async find(logsCollection, logId) {
    const dbLog = await logsCollection.findOne({
      _id: logId,
    });

    if (!dbLog) {
      throw new Error(`could not find the log for logId: '${logId}'`);
    }

    const logs = dbLog.items || [];

    return {
      read() {
        return logs;
      },
      async log(data) {
        let hasResult = false;
        let memoryValue = null;

        if (data.result && data.result instanceof FinishSaga) {
          // eslint-disable-next-line no-param-reassign
          data.result = data.result.value;
        }

        if (data.result && data.result instanceof TransactionValue) {
          hasResult = true;
          memoryValue = data.result.memory;

          // eslint-disable-next-line no-param-reassign
          data.result = data.result.store;
        }

        logs.push(data);

        const logUpdateResult = await logsCollection.findOneAndUpdate({
          _id: logId,
        }, {
          $set: {
            items: logs,
          },
        });

        if (!logUpdateResult.value) {
          throw new Error(`someone else rolled back the saga, so there is no log available. logId: '${logId}'`);
        }

        if (hasResult && memoryValue) {
          // eslint-disable-next-line no-param-reassign
          data.result = memoryValue;

          logs.push(data);
        }

        return logUpdateResult;
      },
    };
  }
}

module.exports = SagaLogger;
