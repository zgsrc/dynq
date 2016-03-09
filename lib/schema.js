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
            function() { return !done; },
            function(cb) {
                me.listSomeTables(last, function(err, data) {
                    if (err) cb(err);
                    else {
                        if (!data.last) done = true;
                        else last = data.last;
                        
                        tables.add(data.tables);
                        cb();
                    }
                });
            },
            function(err) {
                cb(err, tables);
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
                var candidates = null;
                if (filter) {
                    if (Object.isFunction(filter) || Object.isRegExp(filter)) {
                        tables = tables.filter(filter);    
                    }
                    else {
                        candidates = { };
                        Object.keys(filter).forEach((key) => { candidates[filter[key].name] = key; });
                        tables = tables.filter((t) => { return candidates[t] != null; });
                    }
                }
                
                if (tables.length) {
                    async.forEachSeries(tables, function(table, cb) {
                        me.describeTable(table, function(err, metadata) {
                            if (err) cb(err);
                            else {
                                var name = candidates ? candidates[table] : table;
                                me.tables[name] = new Table(me, metadata);
                                cb();
                            }
                        });
                    }, cb);
                }
                else cb();
            }
        });
    };
    
    this.define = function(definition) {
        me.definition = definition;
        return me;
    };
    
    this.defineFromFile = function(filepath) {
        me.definition = JSON.parse(fs.readFileSync(filepath).toString());
        return me;
    };
    
    this.require = function(filepath) {
        var stat = fs.lstatSync(filepath);
        if (stat.isDirectory()) {
            fs.readDirSync(filepath).filter(/.*js/i).forEach(function(file) {
                var tableName = path.basename(file, '.js');
                me.definition[tableName] = require(path.join(filepath, file));
            });
        }
        else {
            var tableName = path.basename(filepath, '.js');
            me.definition[tableName] = require(filepath);
        }
        
        return me;
    };
    
    this.create = function(cb) {
        me.load(me.definition, function(err) {
            if (err) cb(err);
            else {
                async.forEachSeries(Object.keys(me.definition), function(table, cb) {
                    if (me.tables[table] == null) {
                        me.createTable(
                            me.definition[table].name, 
                            me.definition[table].columns,
                            me.definition[table].key,
                            me.definition[table].read,
                            me.definition[table].write,
                            me.definition[table].indices,
                            me.definition[table].locals,
                            function(err) {
                                if (err) cb(err);
                                else {
                                    me.describeTable(me.definition[table].name, function(err, metadata) {
                                        if (err) cb(err);
                                        else {
                                            me.tables[table] = new Table(me, metadata);
                                            
                                            var Methods = me.definition[table].methods;
                                            if (Methods) {
                                                var methods = new Methods(me.tables[table]);
                                                for (var p in methods) {
                                                    if (Object.isFunction(methods[p]) && me.tables[table][p] == null) {
                                                        me.tables[table][p] = methods[p];
                                                    }
                                                }
                                            }
                                            
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
        var tables = Object.values(me.definition).map("name").union(Object.values(me.tables).map("name"));
        async.forEachSeries(tables, function(table, cb) {
            me.deleteTable(table, cb);
        }, cb);
    };
    
    this.backup = function(dir, cb) {
        fs.lstat(dir, function(err, stat) {
            var isDirectory = (!err && stat.isDirectory());
            if (!isDirectory) {
                cb(new Error(dir + " does not exists as a directory."));
            }
            else {
                async.forEachSeries(Object.values(me.tables), function(table, cb) {
                    table.save(path.join(dir, table.name + ".json"), cb);
                }, cb);
            }
        });
    };
    
    this.restore = function(dir, cb) {
        fs.lstat(dir, function(err, stat) {
            var isDirectory = (!err && stat.isDirectory());
            if (!isDirectory) {
                cb(new Error(dir + " does not exists as a directory."));
            }
            else {
                async.forEachSeries(Object.values(me.tables), function(table, cb) {
                    table.load(path.join(dir, table.name + ".json"), cb);
                }, cb);
            }
        });
    };
    
};