module.exports = class FinishSaga {
  constructor({
    store,
    memory,
  }) {
    this.store = store;
    this.memory = memory;
  }
};
