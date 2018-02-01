const _ = require('lodash');
const {
  Collection,
} = require('mongodb');

const utils = {
  createEndlessLoop(callback, timeout) {
    return utils.createConditionalLoop(async next => {
      await callback();
      next();
    }, timeout);
  },
  createConditionalLoop(callback, timeout) {
    const start = () => {
      return new Promise((resolve, reject) => {
        const next = () => {
          setTimeout(async () => {
            try {
              await callback(() => {
                next();
              }, value => {
                resolve(value);
              });
            } catch (ex) {
              reject(ex);
            }
          }, timeout);
        };

        next();
      });
    };

    return {
      start,
    };
  },
  validateSaga(saga) {
    if (!saga) {
      throw new Error('saga not found.');
    }

    if (!saga.id || !_.isString(saga.id)) {
      throw new Error('saga must have an `id` field of type `String`');
    }

    if (!saga.flow) {
      throw new Error('saga must have a `flow` field of type `Object`');
    }

    let propCount = 0;
    for (const propKey in saga.flow) {
      const prop = saga.flow[propKey];

      if (!prop.id || !_.isString(prop.id)) {
        throw new Error('saga `flow item` must have an `id` field of type `String`');
      }

      if (!prop.transaction || !_.isFunction(prop.transaction)) {
        throw new Error('saga `flow item` must have a `transaction` field of type `Function`');
      }

      if (!prop.compensation || !_.isFunction(prop.compensation)) {
        throw new Error('saga `flow item` must have a `compensation` field of type `Function`');
      }

      propCount += 1;
    }

    if (propCount < 2) {
      throw new Error('saga `flow` prop must at least have `2` transaction/compensation pairs');
    }
  },
  isMongoCollection: (object) => object instanceof Collection,
};

module.exports = utils;
