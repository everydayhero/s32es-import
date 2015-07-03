var AWS = require("aws-sdk");

var s3 = new AWS.S3({
  accessKeyId: process.env.AWS_KEY,
  secretAccessKey: process.env.AWS_SECRET
});

var elasticsearchUrl = process.env.ELASTICSEARCH_URL;
var s3Bucket = process.env.S3_BUCKET;
var debug = process.env.DEBUG;
var importer = require("./importer")(s3, {debug: debug});

if (!elasticsearchUrl) {
  console.log("You must specify ELASTICSEARCH_URL");
  process.exit(1);
}

if (s3Bucket) {
  importer.importFolder(s3Bucket, elasticsearchUrl).then(function(data) {
    console.log("Finished importing from bucket", s3Bucket, "to", elasticsearchUrl);
    console.log("Summary:", data);
  }, function(error) {
    if (typeof(error) === "object" && error.message) {
      console.error(error.message);
      if (error.stack) {
        console.error(error.stack);
      }
    } else {
      console.error(error);
    }
    process.exit(1);
  });
} else {
  console.error("You must specify S3_BUCKET");
  process.exit(1);
}
