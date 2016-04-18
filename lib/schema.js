require("sugar");

var async = require("async"),
    fs = require("fs"),
    path = require("path"),
    LineReader = require('file-line-reader'),
    Table = require("./table");

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
        
        me.connection.listTables(options, function(err, data) {
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
    
    this.createTable = function(definition, cb) {
        var options = {
            TableName: definition.name,
            ProvisionedThroughput: { 
                ReadCapacityUnits: definition.read || 5, 
                WriteCapacityUnits: definition.write || 5 
            }
        };
        
        if (!definition.name) {
            cb(new Error("No table name specified."));
            return;
        }

        var columns = { },
            hash = null;

        if (definition.key) {
            var cols = Object.keys(definition.key);
            hash = cols[0];
            options.KeySchema = [ { AttributeName: hash, KeyType: "HASH" } ];
            if (cols.length > 1) {
                options.KeySchema.push({ AttributeName: cols[1], KeyType: "RANGE" });
            }

            columns = Object.merge(columns, definition.key);
        }
        else {
            cb(new Error("No primary key is defined."));
            return;
        }

        if (definition.sorts) {
            options.LocalSecondaryIndexes = Object.keys(definition.sorts).map(function(name) {
                var index = definition.sorts[name],
                    indexOptions = {
                        IndexName: name,
                        KeySchema: [ options.KeySchema.first() ]
                    };

                var cols = Object.keys(index.columns);
                indexOptions.KeySchema.push({ AttributeName: cols[0], KeyType: "RANGE" });

                if (Array.isArray(index.project)) {
                    indexOptions.Projection = { NonKeyAttributes: index.project, ProjectionType: "INCLUDE" };
                }
                else if (index.project) {
                    indexOptions.Projection = { ProjectionType: index.project };
                }
                else {
                    indexOptions.Projection = { ProjectionType: "KEYS_ONLY" };
                }

                columns = Object.merge(columns, index.columns);

                return indexOptions;
            });
        }

        if (definition.indices) {
            options.GlobalSecondaryIndexes = Object.keys(definition.indices).map(function(name) {
                var index = definition.indices[name],
                    indexOptions = {
                        IndexName: name,
                        ProvisionedThroughput: { 
                            ReadCapacityUnits: index.read || 5, 
                            WriteCapacityUnits: index.write || 5
                        }
                    };

                var cols = Object.keys(index.columns);
                indexOptions.KeySchema = [ { AttributeName: cols[0], KeyType: "HASH" } ];
                if (cols.length > 1) {
                    indexOptions.KeySchema.push({ AttributeName: cols[1], KeyType: "RANGE" });
                }

                if (Array.isArray(index.project)) {
                    indexOptions.Projection = { NonKeyAttributes: index.project, ProjectionType: "INCLUDE" };
                }
                else if (index.project) {
                    indexOptions.Projection = { ProjectionType: index.project };
                }
                else {
                    indexOptions.Projection = { ProjectionType: "KEYS_ONLY" };
                }

                columns = Object.merge(columns, index.columns);

                return indexOptions;
            });
        }

        options.AttributeDefinitions = Object.keys(columns).map(function(name) {
            return { AttributeName: name, AttributeType: exports.dataTypes[columns[name]] };
        });

        me.connection.createTable(options, function(err, data) {
            if (err) cb(err);
            else if (data != null) {
                var status = data.TableDescription.TableStatus;
                if (status == "CREATING") {
                    me.connection.waitFor('tableExists', { TableName: definition.name }, function(err, data) {
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
        me.connection.deleteTable({
            TableName: table
        }, function(err) {
            if (err) cb();
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
        me.connection.describeTable({
            TableName: table
        }, cb);
    };
    
    this.changeThroughput = function(table, read, write, cb) {
        me.connection.updateTable({
            TableName: table,
            ProvisionedThroughput: {
                ReadCapacityUnits: read,
                WriteCapacityUnits: write
            }
        }, function(err) {
            if (err) cb(err);
            else {
                var interval = setInterval(function() {
                    me.describeTable(table, function(err, data) {
                        if (data && data.Table.TableStatus == "ACTIVE") {
                            clearInterval(interval);
                            cb();
                        }
                    });
                }, 2500);
            }
        });
    };
    
    this.changeIndexThroughput = function(table, index, read, write, cb) {
        me.connection.updateTable({
            TableName: table,
            GlobalSecondaryIndexUpdates: [
                { 
                    Update: {
                        IndexName: index,
                        ProvisionedThroughput: {
                            ReadCapacityUnits: read,
                            WriteCapacityUnits: write
                        }            
                    }
                }
            ]
        }, function(err) {
            if (err) cb(err);
            else {
                var interval = setInterval(function() {
                    me.describeTable(table, function(err, data) {
                        if (data && data.Table.TableStatus == "ACTIVE") {
                            clearInterval(interval);
                            cb();
                        }
                    });
                }, 2500);
            }
        });
    };
    
    this.factorThroughput = function(description, factor, cb) {
        if (!Object.isNumber(factor)) {
            cb(new Error(factor + " is not a number."));
        }
        else {
            var update = {
                TableName: description.Table.TableName,
                ProvisionedThroughput: {
                    ReadCapacityUnits: Math.round(factor * description.Table.ProvisionedThroughput.ReadCapacityUnits),
                    WriteCapacityUnits: Math.round(factor * description.Table.ProvisionedThroughput.WriteCapacityUnits)
                }
            };

            if (description.Table.GlobalSecondaryIndexes.length) {
                update.GlobalSecondaryIndexUpdates = [ ];
                description.Table.GlobalSecondaryIndexes.forEach(function(index) {
                    update.GlobalSecondaryIndexUpdates.push({ 
                        Update: {
                            IndexName: index.IndexName,
                            ProvisionedThroughput: {
                                ReadCapacityUnits: Math.round(factor * index.ProvisionedThroughput.ReadCapacityUnits),
                                WriteCapacityUnits: Math.round(factor * index.ProvisionedThroughput.WriteCapacityUnits)
                            }            
                        }
                    });
                });
            }

            me.connection.updateTable(update, function(err) {
                if (err) cb(err);
                else {
                    var interval = setInterval(function() {
                        me.describeTable(description.Table.TableName, function(err, data) {
                            if (data && data.Table.TableStatus == "ACTIVE") {
                                clearInterval(interval);
                                cb();
                            }
                        });
                    }, 2500);
                }
            });
        }
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
                        Object.keys(filter).forEach((key) => { 
                            candidates[filter[key].name || filter[key].toString()] = key; 
                        });
                        
                        tables = tables.filter((t) => { return candidates[t] != null; });
                    }
                }
                
                if (tables.length) {
                    async.forEachSeries(tables, function(table, cb) {
                        me.describeTable(table, function(err, metadata) {
                            if (err) cb(err);
                            else {
                                var name = (candidates ? candidates[table] : table);
                                me.tables[name] = new Table(me, metadata);
                                cb();
                            }
                        });
                    }, cb);
                }
                else cb();
            }
        });
        
        return me;
    };
    
    this.define = function(definition) {
        me.definition = Object.merge(me.definition, definition);
        return me;
    };
    
    this.require = function(filepath, options) {
        var stat = fs.lstatSync(filepath);
        if (stat.isDirectory()) {
            fs.readdirSync(filepath).filter(/.*js/i).forEach(function(file) {
                var tableName = path.basename(file, '.js'),
                    table = null; 
                
                try {
                    table = require(path.join(filepath, file));
                }
                catch (ex) {
                    throw new Error("Error loading schema from " + file + ". " + ex.message);
                }
                
                if (Object.isFunction(table)) {
                    table = table(options || {});
                }
                
                me.definition[tableName] = table;
            });
        }
        else {
            var tableName = path.basename(filepath, '.js'),
                table = require(filepath);
            
            if (Object.isFunction(table)) {
                table = table(options || {});
            }
            
            me.definition[tableName] = table;
        }
        
        return me;
    };
    
    this.create = function(options, cb) {
        if (cb == null && Object.isFunction(options)) {
            cb = options;
            options = { };
        }
        
        options.prefix = options.prefix || "";
        Object.values(me.definition).forEach(function(table) {
            if (!table.prefix && options.prefix != "") {
                table.name = options.prefix + table.name;
                table.prefix = options.prefix;
            }
            
            if (options.minReadCapacity) table.read = Math.max(table.read || 5, options.minReadCapacity);
            if (options.minWriteCapacity) table.write = Math.max(table.write || 5, options.minWriteCapacity);
            
            if (table.indices) {
                Object.values(table.indices).forEach(function(index) {
                    if (options.minReadCapacity) index.read = Math.max(index.read || 5, options.minReadCapacity);
                    if (options.minWriteCapacity) index.write = Math.max(index.write || 5, options.minWriteCapacity);
                });
            }
            
            if (table.behaviors) {
                if (!Array.isArray(table.mixins)) {
                    table.mixins = [ ];
                }
                
                table.behaviors.forEach(behavior => {
                    table.mixins.push(behavior(me.definition[table]));
                });
            }
        });
        
        me.load(me.definition, function(err) {
            if (err) cb(err);
            else {
                async.forEachSeries(Object.keys(me.definition), function(table, cb) {
                    if (me.tables[table] == null) {
                        me.createTable(
                            me.definition[table],
                            function(err) {
                                if (err) cb(err);
                                else {
                                    me.describeTable(me.definition[table].name, function(err, metadata) {
                                        if (err) cb(err);
                                        else {
                                            me.tables[table] = new Table(me, metadata);
                                            if (me.definition[table].methods) {
                                                me.tables[table].mixin(me.definition[table].methods);    
                                            }
                                            
                                            if (me.definition[table].mixins) {
                                                me.tables[table].mixins.forEach(mixin => {
                                                    me.tables[table].mixin(mixin);
                                                });
                                            }    
                                            
                                            cb();
                                        }
                                    });
                                }
                            }
                        );
                    }
                    else {
                        if (me.definition[table].methods) {
                            me.tables[table].mixin(me.definition[table].methods);    
                        }
                        
                        if (me.definition[table].mixins) {
                            me.definition[table].mixins.forEach(mixin => {
                                me.tables[table].mixin(mixin);
                            });
                        }
                        
                        cb();
                    }
                }, err => { cb(err, me); });
            }
        });
        
        return me;
    };
    
    this.drop = function(cb) {
        var tables = Object.values(me.definition).map("name").union(Object.values(me.tables).map("name"));
        async.forEachSeries(tables, function(table, cb) {
            me.deleteTable(table, cb);
        }, cb);
        
        return me;
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // FILE STORAGE METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.backup = function(dir, cb) {
        fs.lstat(dir, function(err, stat) {
            var isDirectory = (!err && stat.isDirectory());
            if (!isDirectory) {
                cb(new Error(dir + " does not exists as a directory."));
            }
            else {
                async.forEachSeries(Object.values(me.tables), function(table, cb) {
                    table.backup(path.join(dir, table.name + ".json"), cb);
                }, cb);
            }
        });
        
        return me;
    };
    
    this.restore = function(dir, cb) {
        fs.lstat(dir, function(err, stat) {
            var isDirectory = (!err && stat.isDirectory());
            if (!isDirectory) {
                cb(new Error(dir + " does not exists as a directory."));
            }
            else {
                async.forEachSeries(Object.values(me.tables), function(table, cb) {
                    table.restore(path.join(dir, table.name + ".json"), cb);
                }, cb);
            }
        });
        
        return me;
    };
    
    this.removeBackupFiles = function(dir, cb) {
        async.forEachSeries(Object.values(me.tables), function(table, cb) {
            fs.unlink(path.join(dir, table.name + ".json"), cb);
        }, cb);
        
        return me;
    };
    
};

exports.dataTypes = {
    
    /* String data type */
    "S": "S",
    "string": "S",
    "text": "S",
    "SS": "SS",
    "string set": "SS",
    "text set": "SS",
    
    /* Numeric data type */
    "N": "N",
    "number": "N",
    "NS": "NS",
    "number set": "NS",
    "numeric set": "NS",
    
    /* Binary data type */
    "B": "B",
    "binary": "B",
    "buffer": "B",
    "bytes": "B",
    "BS": "BS",
    "binary set": "BS",
    "buffer set": "BS",
    "bytes set": "BS",
    
    /* Boolean data type */
    "BOOL": "BOOL",
    "bool": "BOOL",
    "boolean": "BOOL",
    
    /* List data type */
    "L": "L",
    "list": "L",
    "array": "L",
    
    /* Map data type */
    "M": "M",
    "map": "M",
    "object": "M",
    "document": "M",
    
    /* Null data type */
    "NULL": "NULL",
    "null": "NULL"
    
};