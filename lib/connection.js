require("sugar");

var async = require("async"),
    AWS = require("aws-sdk"),
    Schema = require("./schema"),
    util = require("./util"),
    decode = util.decode,
    encode = util.encode;

exports.debug = false;

exports.config = function(config) {
    if (config) {
        AWS.config.update(config);
    }
    
    return exports;
};

exports.configFromPath = function(configFilePath) {
    if (configFilePath) {
        AWS.config.loadFromPath(configFilePath);
    }
    
    return exports;
};

exports.connect = function(regions, distribute) {
    return new Connection(regions, distribute);
}

var Connection = exports.Connection = function(regions, distribute) {
    
    var me = this;
    
    me.distributeReads = false,
    me.destinations = [ ],
    me.debug = false;    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // CONFIGURATION
    ////////////////////////////////////////////////////////////////////////////////////////////
    if (regions) {
        if (!Array.isArray(regions)) regions [ regions ];
        me.destinations = regions.map(function(region) {
            return new AWS.DynamoDB({ region: region })
        });
    }
    else {
        me.destinations = [ new AWS.DynamoDB() ];
    }
    
    if (distribute) {
        me.distributeReads = distribute;
    }
    
    me.addRegion = function(region) {
        me.destinations.push(new AWS.DynamoDB({ region: region }));
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // NATIVE OPERATIONS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.batchGetItem = safeRead("batchGetItem", me.destinations, me.distributeReads);
    this.batchWriteItem = safeWrite("batchWriteItem", me.destinations, me.distributeReads);
    this.createTable = safeWrite("createTable", me.destinations, me.distributeReads);
    this.deleteItem = safeWrite("deleteItem", me.destinations, me.distributeReads);
    this.deleteTable = safeWrite("deleteTable", me.destinations, me.distributeReads);
    this.describeTable = safeRead("describeTable", me.destinations, me.distributeReads);
    this.getItem = safeRead("getItem", me.destinations, me.distributeReads);
    this.listTables = safeRead("listTables", me.destinations, me.distributeReads);
    this.putItem = safeWrite("putItem", me.destinations, me.distributeReads);
    this.query = safeRead("query", me.destinations, me.distributeReads);
    this.scan = safeRead("scan", me.destinations, me.distributeReads);
    this.updateItem = safeWrite("updateItem", me.destinations, me.distributeReads);
    this.updateTable = safeWrite("updateTable", me.destinations, me.distributeReads);
    
    this.waitFor = function(event, options, cb) {
        async.forEach(me.destinations, function(dest, cb) {
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

        if (me.debug) {
            console.log("putItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.putItem(options, cb);
    };
    
    this.writeAll = function(table, items, cb) {
        var groups = items.inGroupsOf(25);
        async.forEach(groups, function(group, cb) {
            var request = { RequestItems: { } };
            request.RequestItems[table] = group.map(function(item) {
                return { PutRequest: { Item: encode(item) } };
            });

            async.whilst(
                function() { return request; },
                function(cb) {
                    if (me.debug) {
                        console.log("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
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
                cb
            );
        }, cb);
    };

    this.insert = function(table, keyAttr, item, cb) {
        var options = {
            TableName: table,
            Item: encode(item)
        };

        options.Expected = { };
        if (Array.isArray(keyAttr)) {
            keyAttr.forEach(function(key) { 
                options.Expected[key] = { Exists: false }; 
            });
        }
        else {
            options.Expected[keyAttr] = { Exists: false };
        }
        
        if (me.debug) {
            console.log("putItem " + JSON.stringify(options, null, '\t'));
        }

        me.putItem(options, cb);
    };
    
    this.insertAll = function(table, keyAttr, items, cb) {
        var groups = items.inGroupsOf(25);
        
        var expected = { };
        if (Array.isArray(keyAttr)) {
            keyAttr.forEach(function(key) { 
                expected[key] = { Exists: false }; 
            });
        }
        else {
            expected[keyAttr] = { Exists: false };
        }
        
        async.forEach(groups, function(group, cb) {
            var request = { RequestItems: { } };
            request.RequestItems[table] = group.map(function(item) {
                return { 
                    PutRequest: { 
                        Item: encode(item),
                        Expected: expected
                    } 
                };
            });

            async.whilst(
                function() { return request; },
                function(cb) {
                    if (me.debug) {
                        console.log("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
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
                cb
            );
        }, cb);
    };

    this.upsert = function(table, keyAttr, item, cb) {
        var options = { 
            TableName: table, 
            Key: encode(util.extract(keyAttr, item)), 
            AttributeUpdates: util.put(encode(item, true))
        };
        
        keyAttr.forEach(function(k) {
            delete options.AttributeUpdates[k]; 
        });
        
        if (me.debug) {
            console.log("putItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.updateItem(options, cb);
    };
    
    this.update = function(table, keyAttr, item, cb) {
        var options = { 
            TableName: table, 
            Key: encode(util.extract(keyAttr, item)), 
            AttributeUpdates: util.put(encode(item, true)),
            Expected: { }
        };
        
        keyAttr.forEach(function(k) { 
            options.Expected[k] = { Value: options.Key[k] }; 
            delete options.AttributeUpdates[k]; 
        });
        
        if (me.debug) {
            console.log("putItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.updateItem(options, cb);
    };
    
    this.exists = function(table, key, cb) {
        var options = {
            TableName: table,
            Key: encode(key),
            AttributesToGet: [ Object.keys(key).first() ]
        };
        
        if (me.debug) {
            console.log("getItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, data.Item != null);
        });

    };
    
    this.get = function(table, key, cb) {
        var options = {
            TableName: table,
            Key: encode(key),
            ConsistentRead: true
        };
        
        if (me.debug) {
            console.log("getItem " + JSON.stringify(options, null, '\t'));
        }

        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Item));
        });
    };

    this.getPart = function(table, key, select, cb) {
        var options = {
            TableName: table,
            Key: encode(key),
            ConsistentRead: true
        };
        
        if (Array.isArray(select)) options.AttributesToGet = select;
        else options.ProjectionExpression = select;

        if (me.debug) {
            console.log("getItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Item));
        });
    };
    
    this.getAll = function(table, keys, select, cb) {
        if (cb == null || Object.isFunction(select)) {
            cb = select;
            select = null;
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

            if (select) {
                if (Array.isArray(select)) request.RequestItems[table].AttributesToGet = select;
                else request.RequestItems[table].ProjectionExpression = select;
            }

            group.forEach(function(key) {
                request.RequestItems[table].Keys.push(encode(key));
            });

            var results = [ ];
            async.whilst(
                function() { return request; },
                function(cb) {
                    if (me.debug) {
                        console.log("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
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
                me.getAll(table, map[table].keys, map[table].select, cb);
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

                var select = map[table].select;
                if (select) {
                    if (Array.isArray(select)) request.RequestItems[table].AttributesToGet = select;
                    else request.RequestItems[table].ProjectionExpression = select;
                }

                map[table].keys.forEach(function(key) {
                    request.RequestItems[table].Keys.push(encode(key));
                });
            }); 

            var results = { };
            async.whilst(
                function() { return request; },
                function(cb) {
                    if (me.debug) {
                        console.log("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
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

    this.delete = function(table, key, expected, cb) {
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
        
        if (me.debug) {
            console.log("deleteItem " + JSON.stringify(options, null, '\t'));
        }

        me.deleteItem(options, function(err) {
            if (err) cb(err);
            else cb();
        });
    };
    
    this.deleteAll = function(table, keys, cb) {
        var groups = keys.inGroupsOf(25);
        async.forEach(groups, function(group, cb) {
            var request = { RequestItems: { } };
            request.RequestItems[table] = group.map(function(key) {
                return { DeleteRequest: { Key: encode(key) } };
            });

            async.whilst(
                function() { return request; },
                function(cb) {
                    if (me.debug) {
                        console.log("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
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
                cb
            );
        }, cb);
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
        if (exports.debug) {
            console.log(operation + " " + JSON.stringify(options, null, '\t'));    
        }
        
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
                        err = new Error(err.code + ": " + operation + " " + JSON.stringify(options, null, '\t'));
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
        if (exports.debug) {
            console.log(operation + " " + JSON.stringify(options, null, '\t'));
        }
        
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
                    err = new Error(err.code + ": " + operation + " " + JSON.stringify(options, null, '\t'));
                }
                
                cb(err, data);
            }
        });
    };
}