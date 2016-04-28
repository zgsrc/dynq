require("sugar");

exports.logger = console.log;

function decode(obj) {
    if (obj == null) return obj;
    else if (Array.isArray(obj)) return obj.map(decode);
    else {
        for (var k in obj) {
            if (obj[k] != null) {
                if (obj[k].S) obj[k] = obj[k].S;
                else if (obj[k].N) obj[k] = parseFloat(obj[k].N)
                else if (obj[k].B) obj[k] = new Buffer(obj[k].B);
                else if (obj[k].SS) obj[k] = obj[k].SS;
                else if (obj[k].BS) obj[k] = obj[k].BS.map(function(o) { return new Buffer(o); });
                else if (obj[k].NS) obj[k] = obj[k].NS.map(parseFloat);
                else if (obj[k].BOOL != null) obj[k] = obj[k].BOOL;
                else if (obj[k].NULL) obj[k] = null;
                else if (obj[k].M) obj[k] = decode(obj[k].M);
                else if (obj[k].L) {
                    obj[k] = obj[k].L.map(function(v) { 
                        var wrap = { };
                        wrap[k] = v;
                        return decode(wrap)[k]; 
                    });
                }
                else exports.logger("Unable to decode dynamo field " + JSON.stringify(obj[k]) + ".");
            }
            //else delete obj[k];
        }
        
        return obj;
    }
}

exports.decode = decode;

function encode(o) {
    if (Object.isBoolean(o)) return { BOOL: o };
    else if (Object.isNumber(o)) return { N: o.toString() };
    else if (Buffer.isBuffer(o)) return { B: o };
    else if (Object.isString(o)) return { S: o };
    else if (Object.isDate(o)) return { S: o.iso() };
    else if (Array.isArray(o)) {
        if (o.length == 0) return { L: [ ] };
        else if (o.all(Buffer.isBuffer)) return { BS: o };
        else if (o.all(Object.isNumber)) return { NS: o.map("toString") };
        else if (o.all(Object.isString)) return { SS: o.map("toString") };
        else {
            return { L: o.map(function(o) { 
                return Object.isObject(o) ? { M: encode(o) } : encode(o);
            }) };
        }
    }
    else if (Object.isObject(o)) {
        var obj = Object.clone(o);
        Object.keys(obj).forEach(function(key) {
            if (Object.isObject(obj[key])) obj[key] = { M: encode(obj[key]) };
            else obj[key] = encode(obj[key]);
        });
        
        return obj;
    }
    else if (o != null) return { S: o.toString() };
    else return { NULL: true };
}

exports.encode = encode;

function put(obj) {
    // Encode whole thing to support null values
    var values = encode(obj); 
    return Object.map(values, function(key) { 
        return { Action: "PUT", Value: values[key] };
    });
}

exports.put = put;

function add(obj) {
    return Object.map(obj, function(key) { 
        return { Action: "ADD", Value: encode(obj[key]) }; 
    });
}

exports.add = add;

function del(obj) {
    return Object.map(obj, function(key) { 
        if (obj[key] != null) {
            return { Action: "DELETE", Value: encode(obj[key]) }; 
        }
        else {
            return { Action: "DELETE" };
        }
    });
}

exports.del = del;

function extract(key, obj) { 
    var val = { };
    if (Object.isObject(obj)) key.forEach(function(k) { val[k] = obj[k]; });
    else val[key.first()] = obj;

    return val;
};

exports.extract = extract;

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

const operators = exports.operators = { };
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

exports.getOperator = getOperator;