# DynQ
AWS DynamoDB query library.  Call it "dink" if you like.

## Features
* Support for multi-master writes and distributed reads.
* Automatic resubmission of 'retryable' and provision throughput errors.
* Transparent encoding to and decoding from DynamoDB typed JSON.
* Schema definition and detection
* Intuitive query builder and execution API

## Get Started
```js
var dynq = require("dynq");

// Configure using standard from a Javascript object or a JSON configuration file.
dynq.config({ accessKeyId: "xxx", secretAccessKey: "yyy", maxRetries: 5 });
dynq.configFromPath("./aws.json");

// Create a connection 
var simple = new dynq.Connection("us-east-1");

// Create a multi-master connection with an array of AWS regions. The second parameter specifies if you would like to distribute reads across regions.
var multiMaster = dynq.connect([ "us-east-1", "us-west-1" ], true);
```

That's all you need to get started.  Find documetnation for standard [AWS configuration options](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property) here.  The connections are API compatible with the AWS SDK.

## API
```js
var dynq = require("dynq");

```