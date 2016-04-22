require("sugar");

var async = require("async"),
    assert = require("assert"),
    fs = require("fs"),
    LineReader = require('file-line-reader'),
    util = require("./util"),
    encode = util.encode,
    decode = util.decode;

module.exports = function(schema, description) {
    
    var me = this,
        name = description.Table.TableName,
        key = description.Table.KeySchema.map("AttributeName"),
        extractKey = function(obj) { return util.extract(key, obj); };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // TABLE-LEVEL MEMBERS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.name = name;
    
    this.schema = schema;
    
    this.description = description;
    
    this.mixin = function(Methods) {
        if (Methods && Object.isFunction(Methods)) {
            var methods = new Methods(me);
            for (var p in methods) {
                if (me[p] == null) me[p] = methods[p];
                else console.warn("Cannot override existing table method " + p + ".");
            }
        }
        
        return me;
    };
    
    this.changeThroughput = function(read, write, cb) {
        me.schema.changeThroughput(me.name, read, write, cb);
    };
    
    this.changeIndexThroughput = function(index, read, write, cb) {
        me.schema.changeIndexThroughput(me.name, index, read, write, cb);
    };
    
    this.factorThroughput = function(factor, cb) {
        me.schema.factorThroughput(me.description, factor, cb);
    };
    
    this.drop = function(cb) { 
        me.schema.deleteTable(me.name, cb); 
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // RECORD-LEVEL METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.write = function(obj, cb) { 
        me.schema.connection.write(name, obj, cb); 
    };
    
    this.writeAll = function(objs, cb) { 
        me.schema.connection.writeAll(name, objs, cb); 
    };
    
    this.insert = function(obj, cb) { 
        me.schema.connection.insert(name, key, obj, cb); 
    };
    
    this.upsert = function(obj, cb) {
        me.schema.connection.upsert(name, key, obj, cb);
    };
    
    this.update = function(obj, cb) {
        me.schema.connection.update(name, key, obj, cb);
    };
    
    this.delete = function(obj, cb) { 
        me.schema.connection.delete(name, extractKey(obj), cb); 
    };
    
    this.deleteAll = function(objs, cb) { 
        me.schema.connection.deleteAll(name, objs.map(extractKey), cb); 
    };
    
    this.deleteIf = function(obj, exp, cb) { 
        me.schema.connection.delete(name, extractKey(obj), exp, cb); 
    };
    
    this.exists = function(obj, cb) { 
        me.schema.connection.exists(name, extractKey(obj), cb); 
    };
    
    this.get = function(obj, cb) { 
        me.schema.connection.get(name, extractKey(obj), cb); 
    };
    
    this.getPart = function(obj, select, cb) { 
        me.schema.connection.getPart(name, extractKey(obj), select, cb); 
    };
    
    this.getAll = function(objs, select, cb) {
        me.schema.connection.getAll(name, objs, select, cb);
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // COMPLEX OPERATIONS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.edit = function(obj) { 
        return new Edit(me.schema.connection, name, extractKey(obj)); 
    };
    
    this.query = function(conditions) { 
        var query = new Query(me.schema.connection, name, key, "query"); 
        return conditions ? query.conditions(conditions) : query;
    };
    
    this.scan = function(filter) { 
        var scan = new Query(me.schema.connection, name, key, "scan"); 
        return filter ? scan.filter(filter) : scan;
    };
    
    this.where = function(conditions) {
        var parameters = Object.keys(conditions);
        parameters.each(key => {
            conditions[key][0] = getOperator(conditions[key][0]);
        });
        
        var query = null;
        if (conditions[key[0]] && conditions[key[0]][0] == "EQ") {
            if (key[1] && conditions[key[1]]) {
                // use primary key
                query = me.query(Object.select(conditions, key)).filter(Object.select(conditions, parameters.exclude(key)));
            }
            else {
                // check sort keys
                description.Table.LocalSecondaryIndexes.forEach(index => {
                    var key = index.KeySchema.map("AttributeName");
                    if (conditions[key[1]]) {
                        // use this local secondary index
                        query = me.index(index.IndexName, Object.select(conditions, key)).filter(Object.select(conditions, parameters.exclude(key)));
                    }
                });
                
                // if not found, use primary key
                if (!query) {
                    query = me.query(Object.select(conditions, key)).filter(Object.select(conditions, parameters.exclude(key)));
                }
            }
        }
        else {
            // try to use secondary index
            description.Table.GlobalSecondaryIndexes.forEach(index => {
                var key = index.KeySchema.map("AttributeName");
                if (conditions[key[0]] && conditions[key[0]][0] == "EQ") {
                    // use this key
                    query = me.query(Object.select(conditions, key)).filter(Object.select(conditions, parameters.exclude(key)));
                }
            });
            
            // possibly have to resort to a scan
            if (!query) {
                query = me.scan(conditions);
            }
        }
        
        return query;
    };
    
    this.index = function(name, conditions) { 
        return me.query(conditions).index(name);
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // ADD INDICES AS METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    if (description.Table.GlobalSecondaryIndexes) {
        description.Table.GlobalSecondaryIndexes.forEach(index => {
            var name = index.IndexName.camelize(false);
            if (me[name] == null) {
                me[name] = (hash, range, op) => {
                    var query = me.query().index(index.IndexName);

                    if (hash) {
                        var key = index.KeySchema.map("AttributeName");
                        return range 
                            ? query.conditions({ [key[0]]: [ "EQ", hash ], [key[1]]: [ op || "EQ", range ] }) 
                            : query.conditions({ [key[0]]: [ "EQ", hash ] });
                    }
                    else return query;
                };
            }
        });
    }
    
    if (description.Table.LocalSecondaryIndexes) {
        me.sort = { };
        description.Table.LocalSecondaryIndexes.forEach(index => {
            me.sort[index.IndexName.camelize(false)] = (hash, range, op) => {
                var query = me.query().index(index.IndexName);
                if (hash) {
                    var key = index.KeySchema.map("AttributeName");
                    return range 
                        ? query.conditions({ [key[0]]: [ "EQ", hash ], [key[1]]: [ op || "EQ", range ] }) 
                        : query.conditions({ [key[0]]: [ "EQ", hash ] });
                }
                else return query;
            };
        });
    }
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // FILE STORAGE METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.backup = function(filepath, cb) {
        var scan = me.scan().consistent(), last = null;
        async.doWhilst(function(cb) {
            scan.page(function(err, data) {
                if (err) cb(err);
                else if (data) {
                    var json = data.items.map(JSON.stringify).join("\n")
                    fs.appendFile(filepath, json + "\n", function(err) {
                        if (err) cb(err);
                        else {
                            last = data.last;
                            scan.start(last);
                            cb();
                        }
                    });
                }
                else cb();
            });
        }, function() { 
            return last != null;
        }, cb);
    };
    
    this.restore = function(filepath, cb) {
        var reader = new LineReader(filepath), 
            json = null,
            items = [ ];
        
        async.doWhilst(function(cb) {
            reader.nextLine(function(err, line) {
                if (err) cb(err);
                else if (line && line.trim() != "") {
                    json = JSON.parse(line);
                    items.push(json);
                    if (items.length >= 25) {
                        me.writeAll(items, function(err) {
                            items = [ ];
                            cb(err);
                        });
                    }
                    else cb();
                }
                else {
                    json = null;
                    cb();
                }
            });
        }, function() {
            return json != null;
        }, function(err) {
            if (err) cb(err);
            else if (items.length) me.writeAll(items, cb);
            else cb();
        });
    };
    
};

function Query(connection, table, key, op) {
    
    var me = this,
        selectItems = false,
        deleteItems = false,
        options = { TableName: table },
        extractKey = (obj) => { return util.extract(key, obj); };
    
    this.index = this.from = function(index) {
        options.IndexName = index;
        return me;
    };
    
    this.conditions = function(conditions) {
        if (conditions) {
            if (Object.isString(conditions)) {
                options.KeyConditionExpression = conditions;
            }
            else {
                options.KeyConditions = Object.map(conditions, function(key) {
                    return {
                        AttributeValueList: conditions[key].from(1).map(encode),
                        ComparisonOperator: getOperator(conditions[key][0])
                    };
                });
            }
        }
        
        return me;
    };
    
    this.filter = this.where = function(filter) {
        if (filter) {
            if (Object.isString(filter)) {
                options.FilterExpression = filter;
            }
            else {
                options[op == "scan" ? "ScanFilter" : "QueryFilter"] = Object.map(filter, function(key) {
                    if (filter[key][1]) {
                        return {
                            AttributeValueList: [ encode(filter[key][1]) ],
                            ComparisonOperator: getOperator(filter[key][0])
                        };
                    }
                    else {
                        return {
                            ComparisonOperator: getOperator(filter[key][0])
                        };
                    }
                });
            }
        }
        
        return me;
    };
    
    this.or = function() {
        options.ConditionalOperator = "OR";
        return me;
    };
    
    this.alias = function(alias, attribute) {
        if (!options.ExpressionAttributeNames) {
            options.ExpressionAttributeNames = { };
        }
        
        if (alias[0] != "#") alias = "#" + alias;
        options.ExpressionAttributeNames[alias] = attribute;
        
        return me;
    };
    
    this.parameter = function(name, value) {
        if (!options.ExpressionAttributeValues) {
            options.ExpressionAttributeValues = { };
        }
        
        if (name[0] != ":") name = ":" + name;
        options.ExpressionAttributeValues[name] = encode(value);
        
        return me;
    };
    
    this.select = function(select) {
        if (select) {
            if (Array.isArray(select)) {
                options.AttributesToGet = select;
                options.Select = "SPECIFIC_ATTRIBUTES";
            }
            else if ([ "ALL_ATTRIBUTES", "COUNT", "ALL_PROJECTED_ATTRIBUTES" ].indexOf(select) >= 0) {
                options.Select = select;
            }
            else if (Object.isString(select)) {
                options.ProjectionExpression = select;
            }
        }
        else {
            selectItems = true;
        }
        
        return me;
    };
    
    this.update = function(editor) {
        me.updater = editor;
    };
    
    this.delete = function() {
        deleteItems = true;
        return me;
    }
    
    this.backwards = function() {
        return me.direction("prev");
    };
    
    this.direction = function(direction) {
        options.ScanIndexForward = (direction == "prev" ? false : true);
        return me;
    };
    
    this.segment = function(segment, total) {
        options.Segment = segment;
        options.TotalSegments = total;
        return me;
    };
    
    this.start = function(start) {
        if (start) options.ExclusiveStartKey = start;
        return me;
    };
    
    this.limit = function(count) {
        if (count) options.Limit = count;
        return me;
    };
    
    this.consistent = function() {
        options.ConsistentRead = true;
        return me;
    };
    
    this.first = function(cb) {
        options.Limit = 1;
        connection[op](options, function(err, data) {
            if (err) cb(err);
            else {
                var item = decode(data.Items).first();
                if (item) {
                    if (selectItems) {
                        connection.get(table, extractKey(item), cb);
                    }
                    else if (deleteItems) {
                        connection.delete(table, extractKey(item), null, cb);
                    }
                    else if (me.updater) {
                        me.updater(new Edit(connection, table, extractKey(item)), cb);
                    }
                    else {
                        cb(null, item);
                    }
                }
                else cb();
            }
        });
    };
    
    this.page = function(cb) {
        connection[op](options, function(err, data) {
            if (err) cb(err);
            else {
                var items = decode(data.Items);
                if (selectItems) {
                    connection.getAll(table, items.map(extractKey), function(err, items) {
                        if (err) cb(err);
                        else cb(null, { items: items, last: data.LastEvaluatedKey, count: items.length });
                    });
                }
                else if (deleteItems) {
                    connection.deleteAll(table, items.map(extractKey), function(err) {
                        if (err) cb(err);
                        else cb(null, { items: items, last: data.LastEvaluatedKey, count: data.Count });
                    });
                }
                else if (me.updater) {
                    async.forEach(items, function(item, cb) {
                        me.updater(new Edit(connection, table, extractKey(item)), cb);
                    }, function(err) {
                        if (err) cb(err);
                        else cb(null, { items: items, last: data.LastEvaluatedKey, count: items.length });
                    });
                }
                else {
                    cb(null, { items: items, last: data.LastEvaluatedKey, count: data.Count });
                }
            }
        });
    };
    
    this.all = function(cb) {
        var last = null, items = [ ], count = 0;
        async.doWhilst(function(cb) {
            me.page(function(err, data) {
                if (err) cb(err);
                else if (data) {
                    last = data.last;
                    me.start(last);
                    items.add(data.items);
                    count += data.count;
                    cb();
                }
                else cb();
            });
        }, function() { 
            return last != null && (!options.Limit || options.Limit > count); 
        }, function(err) {
            if (err) cb(err);
            else {
                items = items.compact(true);
                cb(null, { items: items, count: count });
            }
        });
    };
    
    this.debug = function(cb) {
        if (cb) cb(null, JSON.stringify(options));
        return me;
    };
    
}

function Edit(connection, table, key) {
    
    var me = this, 
        options = { 
            TableName: table, 
            Key: encode(key), 
            AttributeUpdates: { } 
        };
    
    this.put = this.change = function(values) {
        Object.merge(options.AttributeUpdates, util.put(values));
        return me;
    };
    
    this.add = function(values) {
        Object.merge(options.AttributeUpdates, util.add(values));
        return me;
    };
    
    this.delete = this.remove = function(values) {
        var obj = { };
        if (Array.create(arguments).all(Object.isString)) {
            Array.create(arguments).each(key => obj[key] = null);
            values = obj;
        }
        else if (Array.isArray(values)) {
            values.each(key => obj[key] = null);
            values = obj;
        }
        
        Object.merge(options.AttributeUpdates, util.del(values));
        return me;
    };
    
    this.conditions = function(expected) {
        if (expected) {
            options.Expected = { };
            
            var ex = encode(expected);
            Object.keys(ex).forEach(function(k) { 
                if (ex[k]) options.Expected[k] = { Value: ex[k] }; 
                else options.Expected[k] = { Exists: false }; 
            });
        }
        
        return me;
    };
    
    this.select = function(select) {
        if (select) options.ReturnValues = select;
        else options.ReturnValues = "ALL_NEW";
        
        return me;
    };
    
    this.update = function(cb) {
        if (!options.Expected) options.Expected = { };
        Object.keys(options.Key).forEach(function(k) { options.Expected[k] = { Value: options.Key[k] }; });
        
        Object.keys(key).forEach(function(k) { delete options.AttributeUpdates[k]; });
        connection.updateItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Attributes));
        });
        
        return me;
    };
    
    this.upsert = function(cb) {
        Object.keys(key).forEach(function(k) { delete options.AttributeUpdates[k]; });
        connection.updateItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Attributes));
        });
        
        return me;
    };
    
    this.debug = function(cb) {
        if (cb) cb(null, JSON.stringify(options));
        return me;
    };
    
}

var operators = { };
operators.eq = operators.equal = operators["="] = operators["=="] = "EQ";
operators.ne = operators["not equal"] = operators["!="] = operators["<>"] = "NE";
operators.le = operators["less than or equal"] = operators["<="] = "LE";    
operators.lt = operators["less than"] = operators["<"] = "LT";
operators.ge = operators["greater than or equal"] = operators[">="] = "GE";    
operators.gt = operators["greater than"] = operators[">"] = "GT";
operators["not null"] = operators["is not null"] = "NOT_NULL";
operators.null = operators["is null"] = "NULL";
operators.contains = "CONTAINS";
operators["not contains"] = operators["does not contain"] = "NOT_CONTAINS";
operators["begins with"] = "BEGINS_WITH";
operators.in = "IN";
operators.between = "BETWEEN";

function getOperator(operator) {
    if (operator && Object.isString(operator)) return operators[operator.spacify().toLowerCase()];
    else return null;
}