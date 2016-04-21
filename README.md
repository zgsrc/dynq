![DynQ](/package.jpg "DynQ")

# DynQ
AWS DynamoDB datastore library.  It makes data access layers based on DynamoDB easier to develop and maintain.  Call it "dink" if you like.  Amongst other things, this library features:

* Schemas
* Queries
* Mixins
* Automatic encoding/decoding of DynamoDB typed JSON format
* Multi-master support

## Installation

    npm install dynq
    
To use dynq, you need an [AWS account](https://aws.amazon.com/).  Once [signed into AWS](https://console.aws.amazon.com/console/home), go to [IAM Security Credentials](https://console.aws.amazon.com/iam/home?#security_credential) section, click on the Access Keys section and get an `access key id` and a `secret access key`.  Use these credentials to configure and connect `dynq` to AWS DynamoDB.

## Get Started

The flexibility of `dynq` comes from using metadata.  A `schema` model uses table definitions and gets existing table metadata to facilitate more seamless programming.

```javascript
var dynq = require("dynq");

// Configure using object or JSON file.
dynq.config({ accessKeyId: "xxx", secretAccessKey: "yyy", maxRetries: 5, region: "us-east-1" });

// Load a schema of tables from file or folder
var schema = dynq.connect().schema().require(path.join(__dirname, "model"), { 
        customize: "reuseable model", 
        configuration: "Setting", 
        enableFeature: true 
    });
    
// Ensure tables exist and are 'active'
schema.create({ 
    prefix: "TABLE_PREFIX_",
    minReadCapacity: 25,
    minWriteCapacity: 20
}, (err) => { /* ready! */ });

// Easily backup and restore data
schema.backup(__dirname + "/directory", (err) => { });
schema.restore(__dirname + "/directory", (err) => { });

// Access tables and data
var table = schema.tables.table;
table.insert({ id: 1, range: 2 }, err => { ... });
table.write({ id: 1, range: 2 }, err => { ... });
table.upsert({ id: 1 range: 2 }, err => { ... });
table.update({ id: 1 range: 3 }, err => { ... });
table.delete(1, err => { ... });
table.exists(1, (err, exists) = > { ... });
table.get(1, (err, item) => { ... });
table.getPart(1, [ "range" ], (err, item) => { ... });

// Query keys-only index and project rest of record
table.query({ id: 1 }).select.all((err, results) => { ... });

// Query and update data
table.query({ id: 1, range: [ "LT", 10 ] })
    .where({ field: [ "BEGINS_WITH", "abc" ] })
    .update((edit, cb) => {
        edit.change({ ... }).add({ ... }).remove({ ... }).upsert(cb);
    }).all(err => { ... });

// Query and delete data
table.query({ id: 1, range: [ "LT", 10 ] })
    .where({ field: [ "BEGINS_WITH", "abc" ] })
    .delete().all(err => { ... });
```

## Configuration

Configure library with standard [AWS configuration options](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property).

* `dynq.config(config)` – Configure the AWS DynamoDB. Chainable with `connect` call.
* `dynq.throughputHandler(destination, table, index)` – Callback on `ProvisionedThroughputExceededException` errors
* `dynq.debug` – Outputs all Dynamo operations to the logger.
* `dynq.logger` – Logger used in conjunction with debug.  Defaults to console.log.

### Connections

```javascript
// Create a simple connection
var cxn = new dynq.Connection({ region: "us-east-1" });

// Create a multi-master connection with an array of AWS regions.
cxn = dynq.connect({ regions: [ "us-east-1", "us-west-1" ], distribute: true });
```

Create connections with the builder method or constructor syntax.

* `dynq.connect(options)`
* `new dynq.Connection(options)`

__Additional Options__

* `regions` - A string or array of AWS regions (e.g. us-east-1).
* `distribute` - A boolean value specifying if reads should be distributed across regions.

### Schemas

Schemas put a programming model around DynamoDB tables using metadata and definition objects.  The easiest way to define a table schema is within a javascript file loaded with the `schema.require` method.

__Schema Example__
```javascript
// index.js
var schema = dynq.connect().schema().require("user.js", { 
    ... /* options */
}).create({ 
    ... /* options */
}, (err, schema) => {
    ... /* ready */
});

// user.js
module.exports = {
    name: "UsersTable",
    key: { id: "string" },
    read: 5,
    write: 5,
    sort: {
        ByUser: {
            columns: { user: "string" },
            project: "ALL"
        }
    },
    indices: {
        ByTimestamp: {
            columns: { timestamp: "number" },
            read: 5,
            write: 5,
            project: "ALL"
        }
    },
    methods: function(table) {
        // These methods will be mixed-in with the table object
        this.foo = function(cb) {
            cb();
        };
    }
};
```

__NOTE:__ The `schema.require` method uses the file name as the identifier within the programming model (i.e. `schema.tables.user`).

The `schema.require` method may take options, which allows development of reuseable table schema generators, or components.  `options` passed to the `schema.require` method are available to a schema definition when defined as a function.

```javascript
// to expose options use a function
module.exports = function(options) {
    return { 
        name: "Users",
        key: { id: "string" },
        read: 5,
        write: 5,
        ...
    };
}
```

Because a schema definition can be thought of as a flexible component, the `schema.create` method also takes options that support customizations like table name prefixes (`prefix`) and read/write capacities (`minReadCapacity` and `minWriteCapacity`).

__Mixins__

A big advantage of schemaless databases like Dynamo is that the data model is readily extensible.  When coupled with method mixins, reuseable behaviors can be composed on tables.

```javascript
// behavior.js
exports.module = function(option) {
    // perform some logic based on options
    var indexName = options.indexName;
    return function(definition) {
        // inspect or alter defintion
        defintion.indicies[indexName] = { columns: { id: "text" } };
        return function(table) {
            // add mixin methods
            this.operation = (cb) => {
                // table exists within this scope
            };
        };
    };
};

// schema.js
exports.module = function(options) {
    // behaviors allow composition of table functionality
    return {
        name: "table",
        key: { id: "string" },
        mixins: require("behavior.js")(options)
    };
};
```

__Programming Model__

A `dyna.schema` provides table creation, modification, and deletion functionality and pares this with table definition and description schemas.

Tables can be defined with the `schema.define` and `schema.require` methods, which merge into the overall `schema.definition` object.  If you have a tables already created, use the `schema.load` to populate the schema from DynamoDB table descriptions.

To bring a schema to life, call the `schema.create` which enumerates through each table in `schema.definition`.  Existing tables are loaded via `schema.load` and ones that do not are created.  All tables can be deleted with `schema.drop`.  The entire schema can be `schema.backup`'d to and `schema.restore`'d from a folder of flat files.

Once a schema has been created, `schema.tables` contain the `dynq.table` objects to be read and written.

```javascript
var schema = dynq.connect("us-east-1").schema();
```

__State__
* `schema.connection` - The underlying connection.
* `schema.tables` - A map of loaded tables.
* `schema.definition` - A definition of tables to be created or loaded.

__Table Creation, Modification, and Deletion Methods__
* `schema.listSomeTables(last, cb)` - List a page of tables starting from last.
* `schema.listAllTables(cb)` - List all tables (automatically page until end).
* `schema.createTable(definition, cb)` - Create a table.  Definition is in format of the _Schema Example_ below.
* `schema.deleteTable(table, cb)` - Deletes a table.
* `schema.describeTable(table, cb)` - Load table metadata.
* `schema.changeThroughput(table, read, write, cb)` - Change throughput for a table.
* `schema.changeIndexThroughput(table, index, read, write, cb)` - Change throughput for an index.
* `schema.factorThroughput(description, factor, cb)` - Factors throughput across the table and its indices.

__Schema Management Methods__
* `schema.load(filter, cb)` - Load tables with names that match filter.
* `schema.define(definition)` - Merge into `schema.definition` from object.
* `schema.require(filepath, [options])` - Loads a table into `schema.definition` from a module.  If a directory is specified, all modules are loaded.
* `schema.create([options,] cb)` - Load tables from `schema.definition` and create ones that do not exist.
* `schema.drop(cb)` - Drop tables from `schema.definition` that exist.
* `schema.backup(dir, cb)` - Saves data from loaded DyanmoDB tables into JSON files.
* `schema.restore(dir, cb)` - Load records into DynamoDB tables from JSON files in the given directory.

### Tables

Tables are accessed through the `schema.table` object and use DynamoDB table description metadata to provide programming abstrations above the low-level DynamoDB API.  With knowledge of the key attributes, a `table` provide conditional record-level methods like `insert`, `update`, and `exists`.  Mass operations like `writeAll`, `deleteAll`, and `getAll` use smart batching logic to gracefully handle DynamoDB operation limitations.

The `table.query` and `table.edit` are builder interfaces for more complicated actions.  The `edit` interface allows for `add` and `remove` operations (in addition to `change`) on individual items.  The `query` interface invokes index queries and table scans, whose data can either be directly returned, or repurposed as keys for a batch select, update, or delete operation.

`table.mixin` enables extension of `table` methods.  Only methods with names who do not already exist will be mixed into the `table`.

```javascript
var schema = dynq.connect("us-east-1").schema();

schema.load(/PREFIX_.*/i, function(err) {
    var table = schema.tables["PREFIX_Users"];
    table.mixin(function(table) {
        this.foo = cb => { cb() };
    });
    
    table.foo(cb => { });
});
```

__Table-Level Members__
* `table.name` - The name of the table.
* `table.schema` - The schema to which this table belongs.
* `table.description` - The metadata from `schema.describeTable(name)`.
* `table.mixin(class)` – Initializes a `class` with this table and mixes in the instance methods.
* `table.changeThroughput(read, write, cb)` - Change throughput for a table.
* `table.changeIndexThroughput(index, read, write, cb)` - Change throughput for an index.
* `table.factorThroughput(factor, cb)` - Factors throughput across the table and its indices.
* `table.drop(cb)` - Drops this table.

__Record-Level Methods__
* `table.write(obj, cb)` - Writes a record to the table.  If a record with the same key already exists, it is overwritten.
* `table.writeAll(objs, cb)` - Writes records to the table.  If a record with the same key already exists, it is overwritten.
* `table.insert(obj, cb)` - Inserts a record into the table.  If a record with the same key already exists, the operation fails.
* `table.delete(key, cb)` - Deletes a record from the table with the given key.
* `table.deleteAll(keys, cb)` - Deletes records from the table with the given keys.
* `table.deleteIf(key, expect, cb)` - Deletes a record from the table with the given key if the expected field values are matched.
* `table.exists(key, cb)` - Indicates if a record with the given key exists.
* `table.get(key, cb)` - Gets the full record that matches the given key.
* `table.getPart(key, select, cb)` - Get part of the record that matches the given key.
* `table.getAll(keys, select, cb)` - Get many records.

__Query Interface__
* `table.query()` - Returns a query interface configured to filter based on an index.
* `table.scan()` - Returns a query interface configured to filter on a table scan.
* `query.index(name)` - The name of an index to query (if not querying the primary key).
* `query.conditions(conditions)` - The conditions on the key and hash of the index.
* `query.start(start)` - Start query from this key.
* `query.limit(count)` - Maximum number of records to query.
* `query.select(select)` - A list of attributes to select, an attribute qualifier, or a projection expression. If empty, the whole record is projected with a separate getItem operation.
* `query.update(editor)` – An update operation to be performed on each item, taking an `edit` interface object and a `cb`.
* `query.delete()` - Delete the queried records.
* `query.backwards()` - Reverse the order in which records are returned.
* `query.direction(direction)` - Set the order in which records are returned.
* `query.filter(filter)` - Set filter conditions on non-indexed fields.
* `query.or()` - Change filter conditions from "and" to "or".
* `query.segment(segment, total)` - Segment a scan operation into parts.
* `query.first(cb)` - Return the first record from the query.
* `query.page(cb)` - Return a page of records.
* `query.all(cb)` - Return all records (automatically paging until the end).
* `query.debug(cb)` - Write the JSON of the query and return it if a cb is supplied.

__Edit Interface__
* `table.edit(obj)` - Returns an edit interface to alter or insert records.
* `edit.change(values)` - Field values to be changed/overwritten.  (Fields without a value are set to null; to remove fields, use remove/delete.)
* `edit.put(values)` – Alias for `edit.change`.
* `edit.add(values)` - Field values to be added.  If numeric, this means atomic addition/subtraction.  If a set, this means addition to the set.
* `edit.remove(values)` - Field values to be removed.  If a set, this means removal from the set.  If an array, denotes a list of fields to be removed.
* `edit.delete(values)` - Alias for `edit.remove`.
* `edit.conditions(expected)` - Values expected to be found in the record.  If not matched, the operation fails.
* `edit.select(select)` - A list of fields to select from the record.
* `edit.update(cb)` - Updates the record.  If a record does not exist, the operation fails.
* `edit.upsert(cb)` - Upserts the record.  If a record does not exists, one is created.
* `edit.debug(cb)` - Write the JSON of the query and return it if a cb is supplied.

__File Methods__
* `table.backup(filepath, cb)` - Save the contents of the DynamoDB table to a file.
* `table.restore(filepath, cb)` - Load the contents of a file to the DynamoDB table.

### Low-Level Connection Interface

The `dynq` module supports the `logger` and `debug` global configuration operations.  The `logger` defaults to `console.log`.  If `debug` is set to true, all DynamoDB native operations are logged.

The `Connection` class provides access to native DynamoDB operations with multi-master support and throughput handling infrastructure.  Some  conditional and mass operations like `insert` and `insertAll` are build on top of the native calls to support higher-level table operations.

##### Configuration

* `cxn.distributeReads` – If multiple masters are specified, each read is dispatched to a randomly selected source.
* `cxn.destinations` – An array of service interfaces used in a multi-master configuration.
* `cxn.debug` –  Outputs all connection operations to the logger.
* `cxn.addDestination(options)` – Adds an additional destination to the `destinations` array after construction.

##### Methods

* `cxn.write(table, item, cb)` – Writes an item.
* `cxn.writeAll(table, items, cb)` – Writes multiple items.
* `cxn.insert(table, keyAttr, item, cb)` – Inserts an item.  If an item with the same key exists, the operation fails.
* `cxn.upsert(table, keyAttr, item, cb)` – Upserts an item.  If the item exists, the fields are merged with the existing item.
* `cxn.update(table, keyAttr, item, cb)` – Updates an item.  If an item with the same key does not exist, the operation fails.
* `cxn.exists(table, key, cb)` – Returns a boolean value indicating if an item with the given key exists.
* `cxn.get(table, key, cb)` – Gets an item with the given key.
* `cxn.getPart(table, key, select, cb)` – Gets part of an item.
* `cxn.getAll(table, keys, select, cb)` – Gets all items with matching keys.
* `cxn.getMany(map, cb)` – Get many items from multple tables.  Map has keys corresponding to table names, and values containing `keys` and `select`.
* `cxn.delete(table, key, expected, cb)` – Deletes a item with the matching key and optionally other expected values.
* `cxn.deleteAll(table, keys, cb)` – Deletes all items with matching keys.

__Arguments__

* `table` - A string specifying the name of a DynamoDB table.
* `keyAttr` - A string or array of key attribute names.
* `key` - An object specifying the unique key of the item.
* `item` - An object representing a table record.
* `select` - An projection expression or an array of strings specifying attributes to get.
* `expected` - An object representing a set of fields that must be matched for the operation to succeed.
* `cb` - Callback with an error and results parameters.

These methods automatically encode parameters to and decode responses from the DynamoDB typed JSON format.

##### Native DynamoDB Operation Proxies

The returned connections are compatible with the [AWS DynamoDB API](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html).

* `cxn.batchGetItem(params, cb)`
* `cxn.batchWriteItem(params, cb)`
* `cxn.createTable(params, cb)`
* `cxn.deleteItem(params, cb)`
* `cxn.deleteTable(params, cb)`
* `cxn.describeTable(params, cb)`
* `cxn.getItem(params, cb)`
* `cxn.listTables(params, cb)`
* `cxn.putItem(params, cb)`
* `cxn.query(params, cb)`
* `cxn.scan(params, cb)`
* `cxn.updateItem(params, cb)`
* `cxn.updateTable(params, cb)`
* `cxn.waitFor(event, options, cb)`

__Arguments__

* `params` - An object specifying query parameters according to AWS Dynamo documentation.
* `cb` - Callback with an error and results parameters.

These methods do not automatically decode responses in the DynamoDB typed JSON format.