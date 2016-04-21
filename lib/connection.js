require("sugar");

var async = require("async"),
    https = require("https"),
    AWS = require("aws-sdk"),
    util = require("./util"),
    Schema = require("./schema");

exports.aws = AWS;
exports.util = util;
exports.debug = false;
exports.logger = console.log;

exports.config = function(config) {
    if (config) AWS.config.dynamodb = config;
    return exports;
};

exports.connect = function(options) {
    return new exports.Connection(options);
};

exports.Connection = function(options) {
    
    var me = this;
    
    me.distributeReads = false,
    me.destinations = [ ],
    me.debug = false;    
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // CONFIGURATION
    ////////////////////////////////////////////////////////////////////////////////////////////
    if (options) {
        if (options.distribute) {
            delete options.distribute;
            me.distributeReads = options.distribute;
        }
        
        if (options.regions) {
            var regions = (Array.isArray(options.regions) ? options.regions : [ options.regions ]);
            delete options.regions;
            me.destinations = regions.map(function(region) {
                options.region = region;
                return new AWS.DynamoDB(options);
            });
        }
        else me.destinations = [ new AWS.DynamoDB(options) ];
    }
    else me.destinations = [ new AWS.DynamoDB() ];
    
    this.addDestination = function(options) {
        me.destinations.push(new AWS.DynamoDB(options));
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // NATIVE OPERATIONS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.batchGetItem = safeRead("batchGetItem", me);
    this.batchWriteItem = safeWrite("batchWriteItem", me);
    this.createTable = safeWrite("createTable", me);
    this.deleteItem = safeWrite("deleteItem", me);
    this.deleteTable = safeWrite("deleteTable", me);
    this.describeTable = safeRead("describeTable", me);
    this.getItem = safeRead("getItem", me);
    this.listTables = safeRead("listTables", me);
    this.putItem = safeWrite("putItem", me);
    this.query = safeRead("query", me);
    this.scan = safeRead("scan", me);
    this.updateItem = safeWrite("updateItem", me);
    this.updateTable = safeWrite("updateTable", me);
    
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
            Item: util.encode(item)
        };

        if (me.debug) {
            exports.logger("putItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.putItem(options, cb);
    };
    
    this.writeAll = function(table, items, cb) {
        var groups = items.inGroupsOf(25).map("compact");
        async.forEachSeries(groups, function(group, cb) {
            var request = { RequestItems: { } };
            request.RequestItems[table] = group.map(function(item) {
                return { PutRequest: { Item: util.encode(item) } };
            });

            async.whilst(
                function() { return request != null; },
                function(cb) {
                    if (me.debug) {
                        exports.logger("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchWriteItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
                            if (data.UnprocessedItems && Object.keys(data.UnprocessedItems) > 0) {
                                request = { RequestItems: data.UnprocessedItems };
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
            Item: util.encode(item)
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
            exports.logger("putItem " + JSON.stringify(options, null, '\t'));
        }

        me.putItem(options, cb);
    };

    this.upsert = function(table, keyAttr, item, cb) {
        var options = { 
            TableName: table, 
            Key: util.encode(util.extract(keyAttr, item)), 
            AttributeUpdates: util.put(item)
        };
        
        keyAttr.forEach(function(k) {
            delete options.AttributeUpdates[k]; 
        });
        
        if (me.debug) {
            exports.logger("updateItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.updateItem(options, cb);
    };
    
    this.update = function(table, keyAttr, item, cb) {
        var options = { 
            TableName: table, 
            Key: util.encode(util.extract(keyAttr, item)), 
            AttributeUpdates: util.put(item),
            Expected: { }
        };
        
        keyAttr.forEach(function(k) { 
            options.Expected[k] = { Value: options.Key[k] }; 
            delete options.AttributeUpdates[k]; 
        });
        
        if (me.debug) {
            exports.logger("updateItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.updateItem(options, cb);
    };
    
    this.exists = function(table, key, cb) {
        var options = {
            TableName: table,
            Key: util.encode(key),
            AttributesToGet: [ Object.keys(key).first() ]
        };
        
        if (me.debug) {
            exports.logger("getItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, data.Item != null);
        });
    };
    
    this.get = function(table, key, cb) {
        var options = {
            TableName: table,
            Key: util.encode(key),
            ConsistentRead: true
        };
        
        if (me.debug) {
            exports.logger("getItem " + JSON.stringify(options, null, '\t'));
        }

        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, util.decode(data.Item));
        });
    };

    this.getPart = function(table, key, select, cb) {
        var options = {
            TableName: table,
            Key: util.encode(key),
            ConsistentRead: true
        };
        
        if (Array.isArray(select)) options.AttributesToGet = select;
        else options.ProjectionExpression = select;

        if (me.debug) {
            exports.logger("getItem " + JSON.stringify(options, null, '\t'));
        }
        
        me.getItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, util.decode(data.Item));
        });
    };
    
    this.getAll = function(table, keys, select, cb) {
        if (cb == null && Object.isFunction(select)) {
            cb = select;
            select = null;
        }
        
        var groups = keys.inGroupsOf(100).map("compact");
        async.mapSeries(groups, function(group, cb) {
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
                request.RequestItems[table].Keys.push(util.encode(key));
            });

            var results = [ ];
            async.whilst(
                function() { return request != null; },
                function(cb) {
                    if (me.debug) {
                        exports.logger("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
                            results.add(data.Responses[table].map(function(result) {
                                return util.decode(result);
                            }));

                            if (data.UnprocessedKeys && Object.keys(data.UnprocessedKeys) > 0) {
                                request = { RequestItems: data.UnprocessedKeys };
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
        if (Object.values(map).map("keys").sum("length") > 100) {
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
                    request.RequestItems[table].Keys.push(util.encode(key));
                });
            }); 

            var results = { };
            async.whilst(
                function() { return request != null; },
                function(cb) {
                    if (me.debug) {
                        exports.logger("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchGetItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
                            Object.keys(data.Responses).forEach(function(table) {
                                if (!results[table]) results[table] = [ ];
                                results[table].add(data.Responses[table].map(function(result) {
                                    return util.decode(result);
                                }));
                            });

                            if (Object.keys(data.UnprocessedKeys) > 0) {
                                request = { RequestItems: data.UnprocessedKeys };
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
            Key: util.encode(key)
        };

        if (expected) {
            options.Expected = { };
            Object.keys(expected).forEach(function(k) { 
                if (expected[k]) options.Expected[k] = { Value: util.encode(expected[k]) }; 
                else options.Expected[k] = { Exists: false }; 
            });
        }
        
        if (me.debug) {
            exports.logger("deleteItem " + JSON.stringify(options, null, '\t'));
        }

        me.deleteItem(options, function(err) {
            if (err) cb(err);
            else cb();
        });
    };
    
    this.deleteAll = function(table, keys, cb) {
        if (!keys || keys.length == 0) {
            cb();
            return;
        }
        
        var groups = keys.inGroupsOf(25).map("compact");
        async.forEachSeries(groups, function(group, cb) {
            var request = { RequestItems: { } };
            request.RequestItems[table] = group.map(function(key) {
                return { DeleteRequest: { Key: util.encode(key) } };
            });

            async.whilst(
                function() { return request != null; },
                function(cb) {
                    if (me.debug) {
                        exports.logger("batchGetItem " + JSON.stringify(request, null, '\t'));
                    }
                    
                    me.batchWriteItem(request, function(err, data) {
                        if (err) cb(err);
                        else {
                            if (data.UnprocessedItems && Object.keys(data.UnprocessedItems) > 0) {
                                request = { RequestItems: data.UnprocessedItems };
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

exports.throughputHandler = function(destination, table, index) {
    exports.logger("Throughput exception on " + table + (index ? " " + index : "") + ".");
};

function safeWrite(operation, cxn) {
    return function(options, cb) {
        if (exports.debug) {
            exports.logger(operation + " " + JSON.stringify(options, null, '\t'));    
        }
        
        async.map(cxn.destinations, function(dest, cb) {
            dest[operation](options, function(err, data) {
                if (err && err.code == "ProvisionedThroughputExceededException") {
                    if (exports.throughputHandler) {
                        exports.throughputHandler(dest, options.TableName, options.IndexName);
                    }
                }

                if (err) {
                    err.operation = operation;
                    err.params = options;
                }

                cb(err, data);
            });
        }, function(err, results) {
            if (exports.debug) {
                if (results) exports.logger(results);
                else if (err) exports.logger(err);
            }
            
            cb(err, results ? results.first() : null);
        });
    };
}

function safeRead(operation, cxn) {
    return function(options, cb) {
        if (exports.debug) {
            exports.logger(operation + " " + JSON.stringify(options, null, '\t'));
        }
        
        var source = (cxn.distributeReads ? cxn.destinations.sample() : cxn.destinations.first());
        source[operation](options, function(err, data) {
            if (err && err.code == "ProvisionedThroughputExceededException") {
                if (exports.throughputHandler) {
                    exports.throughputHandler(source, options.TableName, options.IndexName);
                }
            }
            
            if (err) {
                err.operation = operation;
                err.params = options;
                if (exports.debug) {
                    exports.logger(err);
                }
            }
            else if (data && exports.debug) {
                exports.logger(data);
            }

            cb(err, data);
        });
    };
}