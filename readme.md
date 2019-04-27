# A Distributed Transaction Library based on Node.js and MongoDB

You can read more of the theory [here](https://blog.bernd-ruecker.com/saga-how-to-implement-complex-business-transactions-without-two-phase-commit-e00aa41a1b1b) and [here](https://youtu.be/0UTOLRTwOX0) is the YouTube video that inspired us to write this library.

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/0UTOLRTwOX0/0.jpg)](https://www.youtube.com/watch?v=0UTOLRTwOX0)

## How to use it
A saga has two properties an `id` and a `flow` array. The `id` is any `string` to uniquely identify the saga within the saga list and the `flow` property is an array of steps the saga should take to finish the transaction.

```javascript
const saga = {
  id: 'mySaga',
  flow: [
    {
      transaction: async ({ initialParams, transactionValues }) => {},
      compensation: async ({ transactionValues, compensationValues }) => {},
    },
    ...
  ]
};
```

Every member must look like the following:
```javascript
{
  transaction: // A `Promise` object that eventually resolves to this step's commit result
  compensation: // A `Promise` object that eventually resolves to this step's rollback result
}
```

You can run SAGAs by creating objects as described above and then listing them as `sagaList` and `sagas` property of the saga runner, then initialize a SAGA runner via the `initialize` function and then call the `start` function to run the SAGA engine:
```javascript
const sagaRunner = await saga.initialize({
  aliveLoopTimeout: // A period to check for saga alive signal - milliseconds
  cleanupLogsLoopTimeout: // A timeout for the clear loop of saga logs - milliseconds
  zombieLoopTimeout: // A timeout for a loop that marks inactive sagas as zombies to be removed - milliseconds
  lockHoldTimeout: // A timeout to check for saga update time and remove the saga if it's dead - milliseconds
  lockAcquisitionRetryTimeout: // A timeout to wait and then retry to acquire lock for a new saga - milliseconds
  keepLogsFor: // How many days to keep the logs - days
  waitInsteadOfStopDuration: // How many milliseconds to wait for a retry if another saga with the same keys is running - milliseconds
  rollbackRetryWarningThreshold: // How many retries should a saga do for compensation and then call the `onTooManyRollbackAttempts` function - count
  rollbackWaitTimeout: // How many milliseconds to wait for another retry of rollback - milliseconds
  onTooManyRollbackAttempts: // A function which will be called after too many rollbacks based on the `rollbackRetryWarningThreshold` property. The function's parameters are: `(saga, params, logs)`
  mongoUrl: // The mongoDB url for the SAGA Runner and Executor to connect to
  mongoDBName: // The database for which the SAGA Executor to work with
  mongoOptions: // This will be passed directly to the mongodb driver (for authentication and other options)
  name: // The name of the Runner (This name must be unique between all SAGA Runners in the distributed application)
  sagaList: // A list of all SAGAs that the runner would know
});

sagaRunner.start();
```

Then you can run any saga by calling the `execute` function:

```javascript
sagaRunner.execute({
  sagaId, // string - The id that you used to create the saga
  keys, // [string] - This array will be used to mark the SAGA territory, any other SAGA with any of these keys as its stop key would not run or would have to wait until this saga finishes executing.
  waitInsteadOfStop, // Boolean - Determines if the SAGA should wait for another SAGA with the same keys as this SAGA's stop keys or stop running.
  initialParams, // The initial parameters passed to SAGA transaction steps as a parameter
  stopKeys, // [string] - This array will be used to determine if this SAGA's executing would somehow influence another SAGA's transaction and therefor it must be stopped or wait until the other SAGA finishes executing
});
```