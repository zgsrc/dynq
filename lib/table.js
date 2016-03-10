require("sugar");

var async = require("async"),
    fs = require("fs"),
    lineReader = require('file-line-reader'),
    util = require("./util"),
    encode = util.encode,
    decode = util.decode;

module.exports = function(schema, description) {
    
    var me = this,
        name = description.Table.TableName,
        key = description.Table.KeySchema.map("AttributeName"),
        extractKey = function(obj) { 
            var val = { };
            if (Object.isObject(obj)) key.forEach(function(k) { val[k] = obj[k]; });
            else val[key.first()] = obj;

            return val;
        };
    
    if (description.Table.GlobalSecondaryIndexes) {
        description.Table.GlobalSecondaryIndexes.forEach(function(index) {
            me[index.IndexName.camelize(false)] = me.query().index(index.IndexName);
        });
    }
    
    me.sort = { };
    if (description.Table.LocalSecondaryIndexes) {
        description.Table.LocalSecondaryIndexes.forEach(function(index) {
            me.sort[index.IndexName.camelize(false)] = me.query().index(index.IndexName);
        });
    }
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // TABLE-LEVEL MEMBERS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.name = name;
    
    this.schema = schema;
    
    this.description = description;
    
    this.changeThroughput = function(read, write, cb) {
        me.schema.changeThroughput(me.name, read, write, cb);
    };
    
    this.changeIndexThroughput = function(index, read, write, cb) {
        me.schema.changeIndexThroughput(me.name, index, read, write, cb);
    };
    
    this.factorThroughput = function(factor, cb) {
        me.schema.factorThroughput(description, factor, cb);
    };
    
    this.drop = function(cb) { 
        schema.connection.deleteTable(name, cb); 
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // RECORD-LEVEL METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.overwrite = function(obj, cb) { 
        schema.connection.write(name, obj, cb); 
    };
    
    this.insert = function(obj, cb) { 
        schema.connection.insert(name, key, obj, cb); 
    };
    
    this.delete = function(obj, cb) { 
        schema.connection.destroy(name, extractKey(obj), cb); 
    };
    
    this.deleteIf = function(obj, exp, cb) { 
        schema.connection.destroy(name, extractKey(obj), exp, cb); 
    };
    
    this.exists = function(obj, cb) { 
        schema.connection.exists(name, extractKey(obj), cb); 
    };
    
    this.get = function(obj, cb) { 
        schema.connection.get(name, extractKey(obj), cb); 
    };
    
    this.getPart = function(obj, attributes, cb) { 
        schema.connection.getPart(name, extractKey(obj), attributes, cb); 
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // BUILDER METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.query = function() { 
        return new Query(schema.connection, name, "query"); 
    };
    
    this.scan = function() { 
        return new Query(schema.connection, name, "scan"); 
    };
    
    this.write = function(obj) { 
        return new Write(schema.connection, name, key, obj); 
    };
    
    this.edit = function(obj) { 
        return new Edit(schema.connection, name, extractKey(obj)); 
    };
    
    
    ////////////////////////////////////////////////////////////////////////////////////////////
    // FILE STORAGE METHODS
    ////////////////////////////////////////////////////////////////////////////////////////////
    this.backup = function(filepath, cb) {
        var scan = me.scan(), last = null;
        async.doWhilst(function(cb) {
            scan.page(function(err, data) {
                if (err) cb(err);
                else if (data) {
                    async.forEach(data.items, function(item, cb) {
                        fs.appendFile(filepath, JSON.stringify(item), cb);
                    }, function(err) {
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
        var reader = new LineReader(filepath), json = null;
        async.doWhilst(function(cb) {
            reader.nextLine(function(err, line) {
                if (err) cb(err);
                else if (line) {
                    json = JSON.parse(line);
                    me.overwrite(json, cb);
                }
            });
        }, function() {
            return json != null;
        }, cb);
    };
    
}

function Query(connection, table, op) {
    
    var me = this, 
        options = { TableName: table };
    
    this.index = function(index) {
        options.IndexName = index;
        return me;
    }
    
    this.alias = function(alias, attribute) {
        if (!options.ExpressionAttributeNames) {
            options.ExpressionAttributeNames = { };
        }
        
        if (alias[0] != ":") alias = ":" + alias;
        options.ExpressionAttributeNames[alias] = attribute;
        
        return me;
    };
    
    this.parameter = function(name, value) {
        if (!options.ExpressionAttributeValues) {
            options.ExpressionAttributeValues = { };
        }
        
        options.ExpressionAttributeValues[name] = encode(value);
        
        return me;
    };
    
    this.conditions = function(conditions) {
        if (conditions) {
            if (Object.isString(conditions)) {
                options.KeyConditionExpression = conditions;
            }
            else {
                options.KeyConditions = Object.map(conditions, function(key) {
                    if (Object.isObject(conditions[key][1])) console.error(conditions[key][1]);

                    return {
                        AttributeValueList: conditions[key].from(1).map(encode),
                        ComparisonOperator: conditions[key][0]
                    };
                });
            }
        }
        
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
    
    this.select = function(select) {
        if (select) {
            if (Array.isArray(select)) {
                options.AttributesToGet = select;
                options.select = "SPECIFIC_ATTRIBUTES";
            }
            else if ([ "ALL_ATTRIBUTES", "COUNT", "ALL_PROJECTED_ATTRIBUTES" ].indexOf(select) >= 0) {
                options.Select = select;
            }
            else {
                options.ProjectionExpression = select;
            }
        }
        
        return me;
    };
    
    this.backwards = function() {
        options.ScanIndexForward = false;
        return me;
    };
    
    this.direction = function(direction) {
        options.ScanIndexForward = (direction == "prev" ? false : true);
        return me;
    };
    
    this.filter = function(filter) {
        if (filter) {
            if (Object.isString(filter)) {
                options.FilterExpression = filter;
            }
            else {
                options[op == "scan" ? "ScanFilter" : "QueryFilter"] = Object.map(filter, function(key) {
                    if (filter[key][1]) {
                        return {
                            AttributeValueList: [ encode(filter[key][1]) ],
                            ComparisonOperator: filter[key][0]
                        };
                    }
                    else {
                        return {
                            ComparisonOperator: filter[key][0]
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
    }
    
    this.segment = function(segment, total) {
        options.Segment = segment;
        options.TotalSegments = total;
        return me;
    }
    
    this.first = function(cb) {
        options.Limit = 1;
        connection[op](options, function(err, data) {
            if (err) cb(err);
            else cb(null, decode(data.Items).first());
        });
    }
    
    this.page = function(cb) {
        connection[op](options, function(err, data) {
            if (err) cb(err);
            else cb(null, { items: decode(data.Items), last: data.LastEvaluatedKey, count: data.Count });
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
            items = items.compact(true);
            cb(err, { count: count, items: items });
        });
    };
    
    this.debug = function(cb) {
        console.info(JSON.stringify(options));
        if (cb) cb(null, JSON.stringify(options));
        return me;
    };
    
}

function Write(connection, table, key, item) {
    
    var me = this, 
        options = {
            TableName: table,
            Item: encode(item)
        };
    
    this.select = function(select) {
        options.ReturnValues = select;
        return me;
    };
    
    this.conditions = function(expected) {
        if (expected) {
            if (!options.Expected) options.Expected = { };
            
            var ex = encode(expected);
            Object.keys(ex).forEach(function(k) { 
                if (ex[k]) options.Expected[k] = { Value: ex[k] }; 
                else options.Expected[k] = { Exists: false }; 
            });
        }
        
        return me;
    };
    
    this.insert = function(cb) {
        if (!options.Expected) options.Expected = { };
        if (Array.isArray(key)) key.forEach(function(key) { options.Expected[key] = { Exists: false }; });
        else options.Expected[key] = { Exists: false };
        
        connection.putItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, item);
        });
    };
    
    this.upsert = function(cb) {
        connection.putItem(options, function(err, data) {
            if (err) cb(err);
            else cb(null, item);
        }); 
    };
    
    this.debug = function(cb) {
        console.info(JSON.stringify(options));
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
    
    this.change = function(values) {
        Object.merge(options.AttributeUpdates, putencode(encode(values, true)));
        return me;
    };
    
    this.add = function(values) {
        Object.merge(options.AttributeUpdates, addencode(encode(values)));
        return me;
    };
    
    this.remove = function(values) {
        Object.merge(options.AttributeUpdates, delencode(encode(values)));
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
        options.ReturnValues = select;
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
        console.info(JSON.stringify(options));
        if (cb) cb(null, JSON.stringify(options));
        return me;
    };
    
}