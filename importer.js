var http = require("https");
var URL = require("url");
var moment = require("moment");
var Promise = require("es6-promise").Promise;

function parseOptions(path) {
  var parts = path.split("/");
  return {
    Bucket: parts.shift(),
    Key: parts.join("/")
  };
}

var ImporterPrototype = {
  queueSize: 10,
  typeKey: "fluentd",

  importFolder: function(from, to) {
    var importer = this;
    var options = parseOptions(from);

    console.log("Importing folder", from);
    return new Promise(function(resolve, reject) {
      var marker = null;
      var value = {count: 0};

      function listObjects() {
        importer.s3.listObjects({Bucket: options.Bucket, Prefix: options.Key, Marker: marker}, function(err, data) {
          if (err) {
            reject(err);
          } else {
            var files = data.Contents.map(function(item) {
              return [options.Bucket, item.Key].join("/");
            });
            importer.importFiles(files, to).then(function() {
              value.count += files.length;
              if (data.IsTruncated) {
                marker = files[files.length - 1];
                listObjects();
              } else {
                resolve(value);
              }
            }, reject);
          }
        });
      }

      listObjects();
    });
  },

  importFiles: function(files, to) {
    var importer = this;

    return new Promise(function(resolve, reject) {
      var value = {count: 0};

      function processQueue() {
        var queue = files.slice(0, importer.queueSize);

        value.count += queue.length;
        files = files.slice(importer.queueSize);

        if (queue.length) {
          console.log("Processing", queue.length, "files");
          queue = queue.map(function(file) {
            return importer.importFile(file, to);
          });
          Promise.all(queue).then(processQueue, reject);
        } else {
          resolve(value);
        }
      }

      processQueue();
    });
  },

  importFile: function(file, to) {
    var importer = this;
    var options = parseOptions(file);

    console.log("Importing file", file);
    return new Promise(function(resolve, reject) {
      importer.s3.getObject(options, function(err, data) {
        if (err) {
          reject(err);
        } else {
          try {
            var str = data.Body.toString("utf8");
            resolve(importer.importRecords(JSON.parse(str), to));
          } catch(ex) {
            reject(ex);
          }
        }
      })
    });
  },

  importRecords: function(records, to) {
    var importer = this;
    var url = URL.parse(to);
    var indexKey = url.pathname.replace(/(^\/|\/$)/, "");
    var bulk = [];

    url.path = url.pathname = "/_bulk";
    url.method = "POST";

    records.forEach(function(record) {
      var timestamp = moment(record["@timestamp"] || record.timestamp || record.time);
      var index = {index: {_index: indexKey, _type: importer.typeKey}};

      record["@timestamp"] = timestamp.format();

      bulk.push(JSON.stringify(index));
      bulk.push(JSON.stringify(record));
    });

    bulk.push("");

    return new Promise(function(resolve, reject) {
      console.log("Sending", bulk.length, "commands to", URL.format({host: url.host, port: url.port, path: url.path, protocol: url.protocol}));
      var req = http.request(url, function(res) {
        var body = "";

        res.setEncoding("utf8");
        res.on("data", function(chunk) {
          body += chunk;
        });
        res.on("end", function() {
          try {
            var data = JSON.parse(body);

            if (res.statusCode === 200) {
              resolve(data);
            } else {
              reject(data);
            }
          } catch(ex) {
            reject(ex);
          }
        });
      });

      req.on("error", reject);
      req.write(bulk.join("\n"));
      req.end();
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
