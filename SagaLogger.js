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

        return logUpdateResult;
      },
    };
  }
}

module.exports = SagaLogger;
