require("sugar");

var async = require("async"),
    fs = require("fs"),
    path = require("path"),
    Table = require("./table"),
    LineReader = require('file-line-reader');

module.exports = function(connection) {
    
    var me = this;
    
    this.connection = connection;
    
    this.tables = { };
    
    this.definition = { };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // TABLE DEFINITION METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.listSomeTables = function(last, cb) {
        var options = { };
        
        if (cb == null && Object.isFunction(last)) {
            cb = last;
            last = null;
        }
        
        if (last) {
            options.ExclusiveStartTableName = last;
        }
        
        connection.listTables(options, function(err, data) {
            if (err) cb(err);
            else cb(null, { tables: data.TableNames, last: data.LastEvaluatedTableName });
        });
    };
    
    this.listAllTables = function(cb) {
        var done = false,
            last = null,
            tables = [ ];

        async.whilst(
            function () { return !done; },
            function (cb) {
                me.listSomeTables(last, function(err, data) {
                    if (err) cb(err);
                    else {
                        if (!data.last) done = true;
                        else last = data.last;
                        
                        tables.add(data.tables);
                    }
                });
            },
            function(err) {
                if (err) cb(err);
                else cb(null, tables);
            }
        );
    };

    this.createTable = function(name, columns, key, read, write, indices, locals, cb) {
        var options = {
            TableName: name,
            ProvisionedThroughput: { ReadCapacityUnits: read, WriteCapacityUnits: write }
        };

        try {
            if (columns) {
                options.AttributeDefinitions = Object.keys(columns).map(function(name) {
                    return { AttributeName: name, AttributeType: columns[name] };
                });
            }

            if (Array.isArray(key)) {
                options.KeySchema = [ { AttributeName: key[0], KeyType: "HASH" } ];
                if (key.length > 1) options.KeySchema.push({ AttributeName: key[1], KeyType: "RANGE" });
            }
            else {
                options.KeySchema = [ { AttributeName: key, KeyType: "HASH" } ];
            }

            if (indices) {
                options.GlobalSecondaryIndexes = indices.map(function(index) {
                    var indexOptions = {
                        IndexName: index[0],
                        ProvisionedThroughput: { 
                            ReadCapacityUnits: index[2], 
                            WriteCapacityUnits: index[3] 
                        }
                    };

                    if (Array.isArray(index[1])) {
                        indexOptions.KeySchema = [ { AttributeName: index[1][0], KeyType: "HASH" } ];
                        if (index[1].length > 1) indexOptions.KeySchema.push({ AttributeName: index[1][1], KeyType: "RANGE" });
                    }
                    else indexOptions.KeySchema = [ { AttributeName: index[1][0], KeyType: "HASH" } ];

                    if (Array.isArray(index[4])) indexOptions.Projection = { NonKeyAttributes: index[4], ProjectionType: "INCLUDE" };
                    else if (index[4]) indexOptions.Projection = { ProjectionType: index[4] };
                    else indexOptions.Projection = { ProjectionType: "KEYS_ONLY" };

                    return indexOptions;
                });
            }

            if (locals) {
                options.LocalSecondaryIndexes = locals.map(function(index) {
                    var indexOptions = {
                        IndexName: index[0],
                        KeySchema: [ options.KeySchema.first() ]
                    };

                    indexOptions.KeySchema.push({ AttributeName: index[1], KeyType: "RANGE" });

                    if (Array.isArray(index[2])) indexOptions.Projection = { NonKeyAttributes: index[2], ProjectionType: "INCLUDE" };
                    else if (index[2]) indexOptions.Projection = { ProjectionType: index[2] };
                    else indexOptions.Projection = { ProjectionType: "KEYS_ONLY" };

                    return indexOptions;
                });
            }
        }
        catch (ex) {
            cb(ex);
            return;
        }

        connection.createTable(options, function(err, data) {
            if (err) cb(err);
            else if (data != null) {
                var status = data.TableDescription.TableStatus;
                if (status == "CREATING") {
                    connection.waitFor('tableExists', { TableName: name }, function(err, data) {
                        cb(null, true);
                    });
                }
                else if (status == "ACTIVE") {
                    cb();
                }
                else {
                    cb(new Error(name + " status is " + status));
                }
            }
            else cb();
        });
    };

    this.deleteTable = function(table, cb) {
        connection.deleteTable({
            TableName: table
        }, function(err) {
            if (err) {
                cb();
            }
            else {
                var interval = setInterval(function() {
                    me.describeTable(table, function(err, data) {
                        if (err) {
                            clearInterval(interval);
                            cb();
                        }
                    });
                }, 2500);
            }
        });
    };

    this.describeTable = function(table, cb) {
        connection.describeTable({
            TableName: table
        }, cb);
    };

    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // SCHEMA MANAGEMENT METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.load = function(filter, cb) {
        if (cb == null && Object.isFunction(filter)) {
            cb = filter;
            filter = null;
        }
        
        me.listAllTables(function(err, tables) {
            if (err) cb(err);
            else {
                if (filter) {
                    tables = tables.filter(filter);
                }
                
                async.forEachSeries(tables, function(table, cb) {
                    me.describeTable(table, function(err, metadata) {
                        if (err) cb(err);
                        else {
                            me.tables[table] = new Table(me, metadata);
                            cb();
                        }
                    });
                }, cb);
            }
        });
    };
    
    this.define = function(definition) {
        me.definition = definition;
        return me;
    };
    
    this.defineFromFile = function(path) {
        me.definition = JSON.parse(fs.readFileSync(path).toString());
        return me;
    };
    
    this.create = function(cb) {
        var tables = Object.keys(me.definition);
        me.load(new RegExp(tables.join('|')), function(err) {
            if (err) cb(err);
            else {
                async.forEachSeries(tables, function(table, cb) {
                    if (me.tables[table] == null) {
                        me.createTable(
                            table, 
                            tables[table].columns,
                            tables[table].key,
                            tables[table].read,
                            tables[table].write,
                            tables[table].indices,
                            tables[table].locals,
                            function(err) {
                                if (err) cb(err);
                                else {
                                    me.describeTable(table, function(err, metadata) {
                                        if (err) cb(err);
                                        else {
                                            me.tables[table] = new Table(me, metadata);
                                            cb();
                                        }
                                    });
                                }
                            }
                        );
                    }
                    else {
                        cb();
                    }
                }, cb);
            }
        });
    };
    
    this.drop = function(cb) {
        var tables = Object.keys(me.definition).union(Object.keys(me.tables));
        async.forEachSeries(tables, function(table, cb) {
            me.deleteTable(table, cb);
        }, cb);
    };
    
    this.backup = function(filepath, cb) {
        fs.lstat(filepath, function(err, stat) {
            var isDirectory = (!err && stat.isDirectory()),
                results = { };
            
            async.forEachSeries(Object.values(me.tables), function(table, cb) {
                table.scan().all(function(err, records) {
                    if (err) cb(err);
                    else {
                        if (isDirectory) {
                            fs.writeFile(path.join(filepath, table.name), JSON.stringify(records, null, '\t'), cb);
                        }
                        else {
                            results[table.name] = records;
                            cb();
                        }
                    }
                })
            }, function(err) {
                if (err) cb(err);
                else {
                    if (!isDirectory) fs.writeFile(filepath, JSON.stringify(results, null, '\t'), cb);
                    else cb();
                }
            });
        });
    };
    
    this.restore = function(filepath, cb) {
        fs.lstat(filepath, function(err, stat) {
            var isDirectory = (!err && stat.isDirectory());
            if (isDirectory) {
                fs.readdir(filepath, function(err, files) {
                    if (err) cb(err);
                    else {
                        files = files.filter(/.*json/gi);
                    }
                }).
            }
            else {
                fs.readFile(filepath, function(err, data) {
                    if (err) cb(err);
                    else {
                        data = JSON.parse(data.toString());
                    }
                });
            }
        });
    };
    
};