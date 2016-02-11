require("sugar");

var async = require("async"),
    AWS = require("aws-sdk"),
    Schema = require("./schema"),
    util = require("./util"),
    decode = util.decode,
    encode = util.encode;

exports.config = function(config) {
    if (config) {
        AWS.config.update(config);
    }
};

exports.configFromPath = function(configFilePath) {
    if (configFilePath) {
        AWS.config.loadFromPath(configFilePath);
    }
};

exports.connect = function(regions, distribute) {
    return new Connection(regions, distribute);
}

var Connection = exports.Connection = function(regions, distribute) {
    
    var me = this,
        distributeReads = me.distributeReads = false,
        destinations = me.destinations = [ ];    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // CONFIGURATION
    ////////////////////////////////////////////////////////////////////////////////////////////
    if (regions) {
        destinations = me.destinations = regions.map(function(region) {
            return new AWS.DynamoDB({ region: region })
        });
    }
    
    if (distribute) {
        distributeReads = me.distributeReads = distribute;
    }
    
    this.setDistributeReads = function(distribute) {
        distributeReads = me.distributeReads = distribute;
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // NATIVE OPERATIONS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.batchGetItem = safeRead("batchGetItem", destinations, distributeReads);
    this.batchWriteItem = safeWrite("batchWriteItem", destinations, distributeReads);
    this.createTable = safeWrite("createTable", destinations, distributeReads);
    this.deleteItem = safeWrite("deleteItem", destinations, distributeReads);
    this.deleteTable = safeWrite("deleteTable", destinations, distributeReads);
    this.describeTable = safeRead("describeTable", destinations, distributeReads);
    this.getItem = safeRead("getItem", destinations, distributeReads);
    this.listTables = safeRead("listTables", destinations, distributeReads);
    this.putItem = safeWrite("putItem", destinations, distributeReads);
    this.query = safeRead("query", destinations, distributeReads);
    this.scan = safeRead("scan", destinations, distributeReads);
    this.updateItem = safeWrite("updateItem", destinations, distributeReads);
    this.updateTable = safeWrite("updateTable", destinations, distributeReads);
    
    this.waitFor = function(event, options, cb) {
        async.forEach(destinations, function(dest, cb) {
            dest.waitFor(event, options, cb);
        }, cb);
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // RECORD-LEVEL OPERATIONS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.exists = function(table, key, cb) {
        var options = {
            TableName: table,
            Key: encode(key),
            ReturnValues: "COUNT"
        };

        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, parseInt(data.Count) > 0);
        });

    };

    this.write = function(table, item, cb) {
        var options = {
            TableName: table,
            Item: encode(item)
        };

        me.putItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(item));
        });
    };

    this.insert = function(table, key, item, cb) {
        var options = {
            TableName: table,
            Item: encode(item)
        };

        options.Expected = { };
        if (Array.isArray(key)) key.forEach(function(key) { options.Expected[key] = { Exists: false }; });
        else options.Expected[key] = { Exists: false };

        me.putItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(item));
        });
    };

    this.get = function(table, key, cb) {
        var options = {
            TableName: table,
            Key: encode(key),
            ConsistentRead: true
        };

        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Item));
        });
    };

    this.getPart = function(table, key, attributes, cb) {
        var options = {
            TableName: table,
            Key: encode(key),
            AttributesToGet: attributes,
            ConsistentRead: true
        };

        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Item));
        });
    };

    this.destroy = function(table, key, expected, cb) {
        if (expected && !cb) {
            cb = expected;
            expected = null;
        }

        var options = {
            TableName: table,
            Key: encode(key)
        };

        if (expected) {
            options.Expected = { };

            var ex = encode(expected);
            Object.keys(ex).forEach(function(k) { 
                if (ex[k]) options.Expected[k] = { Value: ex[k] }; 
                else options.Expected[k] = { Exists: false }; 
            });
        }

        me.deleteItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Attributes));
        });
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // SCHEMA ABSTRACTION
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.schema = function() {
        return new Schema(me);
    };
    
};

function safeWrite(operation, destinations, distributeReads) {
    return function(options, cb) {
        async.map(destinations, function(dest, cb) {
            dest[operation](options, function(err, data) {
                if (err && err.code == "ProvisionedThroughputExceededException") {
                    var name = options.TableName + (options.IndexName ? "." + options.IndexName : "");
                    console.warn("AWS DynamoDB operation " + operation + " on " + name + " encountered a throughput exception.  Retrying operation...");
                    dest[operation](options, function(err, data) {
                        if (err && err.code == "ProvisionedThroughputExceededException") {
                            console.error("AWS DynamoDB retry operation still encountered throughput exception.  Recommend increasing throughput on " + name + ".");
                            cb(err, data);
                        }
                        else cb(err, data);
                    });
                }
                else if (err && err.retryable) {
                    var name = options.TableName + (options.IndexName ? "." + options.IndexName : "");
                    console.warn("Retrying AWS DynamoDB operation " + operation + " on " + name + " after encountering an " + err.code + " exception.  Retrying operation...");
                    dest[operation](options, function(err, data) {
                        if (err) {
                            console.error("AWS DynamoDB error persists for operation " + operation + " on " + name + ".");
                            console.error(err);
                            cb(err, data);
                        }
                        else cb(err, data);
                    });
                }
                else {
                    if (err) {
                        err = new Error(err.code + ": " + operation + " " + JSON.stringify(options));
                    }

                    cb(err, data);
                }
            });
        }, function(err, results) {
            cb(err, results ? results.first() : null);
        });
    };
}

function safeRead(operation, destinations, distributeReads) {
    return function(options, cb) {
        var source = (distributeReads ? destinations.sample() : destinations.first());
        source[operation](options, function(err, data) {
            if (err && err.code == "ProvisionedThroughputExceededException") {
                var name = options.TableName + (options.IndexName ? "." + options.IndexName : "");
                console.warn("AWS DynamoDB operation " + operation + " on " + name + " encountered a throughput exception.  Retrying operation...");
                source[operation](options, function(err, data) {
                    if (err && err.code == "ProvisionedThroughputExceededException") {
                        console.error("AWS DynamoDB retry operation still encountered throughput exception.  Recommend increasing throughput on " + name + ".");
                        cb(err, data);
                    }
                    else cb(err, data);
                });
            }
            else if (err && err.retryable) {
                var name = options.TableName + (options.IndexName ? "." + options.IndexName : "");
                console.warn("Retrying AWS DynamoDB operation " + operation + " on " + name + " after encountering an " + err.code + " exception.  Retrying operation...");
                source[operation](options, function(err, data) {
                    if (err) {
                        console.error("AWS DynamoDB error persists for operation " + operation + " on " + name + ".");
                        console.error(err);
                        cb(err, data);
                    }
                    else cb(err, data);
                });
            }
            else {
                if (err) {
                    err = new Error(err.code + ": " + operation + " " + JSON.stringify(options));
                }
                
                cb(err, data);
            }
        });
    };
}