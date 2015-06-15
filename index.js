var AWS = require("aws-sdk");

var s3 = new AWS.S3({
  accessKeyId: process.env.AWS_KEY,
  secretAccessKey: process.env.AWS_SECRET
});

var importer = require("./importer")(s3);

if (!process.env.ELASTICSEARCH_URL) {
  console.log("You must specify ELASTICSEARCH_URL");
  process.exit(1);
}

if (process.env.S3_BUCKET) {
  importer.importFolder(process.env.S3_BUCKET, process.env.ELASTICSEARCH_URL).then(function(data) {
    console.log("Imported", data.count, "files");
  }, function(error) {
    console.error(error);
    process.exit(1);
  });
} else {
  console.error("You must specify S3_BUCKET");
  process.exit(1);
}
