var Promise = require("es6-promise").Promise;

module.exports = {
  batch: function(list, callback, size) {
    var items = list.slice(0);

    size = size || list.length

    return new Promise(function(resolve, reject) {
      var results = [];

      (function process() {
        var queue = items.slice(0, size);
        items = items.slice(size);

        if (!queue.length) {
          resolve(results);
          return;
        }

        Promise.all(queue.map(callback)).then(function(queueResults) {
          results.push.apply(results, queueResults);
          process();
        }, reject);
      })();
    });
  },

  while: function(callback) {
    return new Promise(function(outerResolve, outerReject) {
      var results = [];

      (function process() {
        var promise = new Promise(function(innerResolve, innerReject) {
          var returnValue = callback(innerResolve, innerReject);

          if (returnValue === false) {
            outerResolve(results);
          } else if (returnValue !== undefined) {
            innerResolve(returnValue);
          }
        });

        promise.then(function(result) {
          results.push(result);
          process();
        }, outerReject);
      })();
    });
  }
};
