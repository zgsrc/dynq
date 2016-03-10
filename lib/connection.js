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
        if (!Array.isArray(regions)) regions [ regions ];
        destinations = me.destinations = regions.map(function(region) {
            return new AWS.DynamoDB({ region: region })
        });
    }
    else {
        destinations = me.destinations = [ new AWS.DynamoDB() ];
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
    // KEY-VALUE STORE OPERATIONS
    ////////////////////////////////////////////////////////////////////////////////////////////
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
        if (Array.isArray(key)) {
            key.forEach(function(key) { 
                options.Expected[key] = { Exists: false }; 
            });
        }
        else {
            options.Expected[key] = { Exists: false };
        }

        me.putItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(item));
        });
    };

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
            ConsistentRead: true
        };
        
        if (Array.isArray(attributes)) options.AttributesToGet = attributes;
        else options.ProjectionExpression = attributes;

        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Item));
        });
    };
    
    this.getAll = function(table, keys, attributes, cb) {
        if (cb == null || Object.isFunction(attributes)) {
            cb = attributes;
            attributes = null;
        }
        
        var groups = keys.inGroupsOf(100);
        async.map(groups, function(group, cb) {
            var request = { 
                RequestItems: { } 
            };

            request.RequestItems[table] = {
                ConsistentRead: true,
                Keys: [ ]
            };

            if (attributes) {
                if (Array.isArray(attributes)) request.RequestItems[table].AttributesToGet = attributes;
                else request.RequestItems[table].ProjectionExpression = attributes;
            }

            group.forEach(function(key) {
                request.RequestItems[table].Keys.push(encode(key));
            });

            var results = [ ];
            async.whilst(
                function() { return request; },
                function(cb) {
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
                            results.add(data.Responses[table].map(function(result) {
                                return decode(result);
                            }));

                            if (Object.keys(data.UnprocessedKeys) > 0) {
                                request = { RequestItems: data.UnprocessedKeys[table] };
                            }
                            else {
                                request = null;
                            }

                            cb();
                        }
                    });
                },
                function(err) {
                    if (err) cb(err);
                    else cb(null, results);
                }
            );
        }, function(err, items) {
            if (err) cb(err);
            else cb(null, items.flatten().compact(true));
        });
    };
    
    this.getMany = function(map, cb) {
        if (Object.values(map).sum("length") > 100) {
            var results = { };
            async.mapSeries(Object.keys(map), function(table, cb) {
                me.getAll(table, map[table].keys, map[table].attributes, cb);
            }, function(err, items) {
                if (err) cb(err);
                else {
                    Object.keys(map).each(function(table, i) {
                        results[table] = items[i];
                    });

                    cb(null, results);
                }
            })
        }
        else {
            var request = { 
                RequestItems: { } 
            };

            Object.keys(map).forEach(function(table) {
                request.RequestItems[table] = {
                    ConsistentRead: true,
                    Keys: [ ]
                };

                var attributes = map[table].attributes;
                if (attributes) {
                    if (Array.isArray(attributes)) request.RequestItems[table].AttributesToGet = attributes;
                    else request.RequestItems[table].ProjectionExpression = attributes;
                }

                map[table].keys.forEach(function(key) {
                    request.RequestItems[table].Keys.push(encode(key));
                });
            }); 

            var results = { };
            async.whilst(
                function() { return request; },
                function(cb) {
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
                            Object.keys(results.Responses).forEach(function(table) {
                                if (!results[table]) results[table] = { };
                                results[table].add(data.Responses[table].map(function(result) {
                                    return decode(result);
                                }));
                            });

                            if (Object.keys(data.UnprocessedKeys) > 0) {
                                request = { RequestItems: data.UnprocessedKeys[table] };
                            }
                            else {
                                request = null;
                            }

                            cb();
                        }
                    });
                },
                function(err) {
                    if (err) cb(err);
                    else cb(null, results);
                }
            );
        }
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