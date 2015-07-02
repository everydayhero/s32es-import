var URL = require("url");
var fetch = require("node-fetch");
var moment = require("moment");
var Promise = require("es6-promise").Promise;
var Queue = require("./queue");

function parseOptions(path) {
  var parts = path.split("/");
  return {
    Bucket: parts.shift(),
    Key: parts.join("/")
  };
}

function summarizeResults(results) {
  var summary = {success: 0, failure: 0, total: 0};

  results.forEach(function(result) {
    summary.success += result.success;
    summary.failure += result.failure;
    summary.total += result.total;
  });

  return summary;
}

var ImporterPrototype = {
  queueSize: 10,
  typeKey: "fluentd",

  importFolder: function(from, to) {
    var importer = this;
    var options = parseOptions(from);
    var marker = null;

    return Queue.while(function(resolve, reject) {
      if (marker === false) {
        return false;
      }

      importer.s3.listObjects({Bucket: options.Bucket, Prefix: options.Key, Marker: marker}, function(err, data) {
        if (err) {
          reject(err);
        } else {
          var contents = data.Contents || [];
          var files = contents.filter(function(item) {
            return item.Key !== marker;
          }).map(function(item) {
            return [options.Bucket, item.Key].join("/");
          });

          importer.importFiles(files, to).then(function(results) {
            if (data.IsTruncated) {
              marker = (contents[contents.length - 1] || {Key: false}).Key;
            } else {
              marker = false;
            }

            resolve(results);
          }, reject);
        }
      });
    }).then(summarizeResults);
  },

  importFiles: function(files, to) {
    var importer = this;

    return Queue.batch(files, function(file) {
      return importer.importFile(file, to);
    }, importer.queueSize).then(summarizeResults);
  },

  importFile: function(file, to) {
    var importer = this;
    var options = parseOptions(file);

    return new Promise(function(resolve, reject) {
      importer.s3.getObject(options, function(err, data) {
        var body = data.Body || new Buffer(0);

        if (err) {
          reject(err);
        } else {
          var str = body.toString("utf8");
          resolve(importer.importRecords(JSON.parse(str), to));
        }
      })
    }).catch(function(error) {
      return Promise.reject([file, error].join(": "));
    });
  },

  importRecords: function(records, to) {
    var importer = this;
    var url = URL.parse(to);
    var indexKey = url.pathname.replace(/(^\/|\/$)/, "");
    var bulk = [];

    url.path = url.pathname = "/_bulk";
    var bulkUrl = URL.format(url);

    records.forEach(function(record) {
      var timestamp = moment(record["@timestamp"] || record.timestamp || record.time);
      var index = {index: {_index: indexKey, _type: importer.typeKey}};

      record["@timestamp"] = timestamp.format();

      bulk.push(JSON.stringify(index));
      bulk.push(JSON.stringify(record));
    });

    bulk.push("");

    return fetch(bulkUrl, {method: "POST", body: bulk.join("\n")}).then(function(res) {
      return res.json().then(function(json) {
        return Promise[res.ok ? "resolve" : "reject"](json);
      });
    }).then(function(results) {
      var summary = {success: 0, failure: 0, total: 0};
      var items = results.items || [];

      items.forEach(function(item) {
        var success = item.create.status === 201;

        summary.total += 1;
        summary[success ? "success" : "failure"] += 1;
      });

      return summary;
    });
  }
};

module.exports = function(s3) {
  return Object.create(ImporterPrototype, {
    s3: {
      value: s3,
      enumerable: false,
      writable: false,
      configurable: false
    }
  });
};
