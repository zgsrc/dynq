require("sugar");

var async = require("async"),
    Table = require("./table");

module.exports = function(connection) {
    
    var me = this;
    
    this.connection = connection;
    
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
                
                var schema = { };
                async.forEachLimit(tables, 5, function(cb) {
                    me.describeTable(table, function(err, metadata) {
                        if (err) cb(err);
                        else {
                            schema[table] = new Table(me, metadata);
                            cb();
                        }
                    });
                }, function(err) {
                    if (err) cb(err);
                    else {
                        me.tables = schema;
                        cb();
                    }
                });
            }
        });
    };
    
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
    
};