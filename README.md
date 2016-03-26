![DynQ](/package.jpg "DynQ")

# DynQ
AWS DynamoDB query library.  It makes data access layers based on DynamoDB easier to develop and maintain.  Call it "dink" if you like.  Amongst other things, this library features:

* Support for multi-master writes and distributed reads.
* Automatic resubmission of 'retryable' and provision throughput errors.
* Transparent encoding to and decoding from DynamoDB typed JSON.
* Schema definition and detection
* Intuitive query builder and execution API

## Get Started
```js
var dynq = require("dynq");

// Configure using object or JSON file.
dynq.config({ accessKeyId: "xxx", secretAccessKey: "yyy", maxRetries: 5 });

// Create a simple connection
var cxn = new dynq.Connection("us-east-1");

// Create a multi-master connection with an array of AWS regions.
cxn = dynq.connect([ "us-east-1", "us-west-1" ], true);
```

## Documentation

### Configuration

Configure library with standard [AWS configuration options](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property).

* `dynq.config(config)` – Configure the AWS library.
* `dynq.dynamo(region)` – Method to create a low-level `AWS.DynamoDB` service interface.
* `dynq.throughputHandler(destination, table, index)` – Callback on `ProvisionedThroughputExceededException` errors
* `dynq.debug` – Outputs all Dynamo operations to the logger.
* `dynq.eproto` – Option to take EPROTO error mitigation measures in `dynamo` method.
* `dynq.logger` – Logger used in conjunction with debug.  Defaults to console.log.

### Connections

Create connections with the builder method or constructor syntax.

* `dynq.connect(regions, distributeReads)`
* `new dynq.Connection(regions, distributeReads)`

__Arguments__

* `regions` - A string or array of AWS regions (e.g. us-east-1).
* `distributeReads` - A boolean value specifying if reads should be distributed across regions.

### Schemas

The schema API puts structure around the definition and querying of DynamoDB tables.

```js
var schema = dynq.connect("us-east-1").schema();
```

__State__
* `schema.connection` - The underlying connection.
* `schema.tables` - A map of loaded tables.
* `schema.definition` - A definition of tables to be created or loaded.

__Table Definition Methods__
* `schema.listSomeTables(last, cb)` - List a page of tables starting from last.
* `schema.listAllTables(cb)` - List all tables (automatically page until end).
* `schema.createTable(definition, cb)` - Create a table.  Definition is in format of the _Schema Example_ below.
* `schema.describeTable(table, cb)` - Load table metadata.
* `schema.changeThroughput(table, read, write, cb)` - Change throughput for a table.
* `schema.changeIndexThroughput(table, index, read, write, cb)` - Change throughput for an index.
* `schema.factorThroughput(description, factor, cb)` - Factors throughput across the table and its indices.

__Schema Management Methods__
* `schema.load(filter, cb)` - Load tables with names that match filter.
* `schema.define(definition)` - Set `schema.definition` from object.
* `schema.require(filepath)` - Loads a table into `schema.definition` from a module.  If a directory is specified, all modules are loaded.
* `schema.create(cb)` - Load tables from `schema.definition` and create ones that do not exist.
* `schema.drop(cb)` - Drop tables from `schema.definition` that exist.
* `schema.backup(dir, cb)` - Saves data from loaded DyanmoDB tables into JSON files.
* `schema.restore(dir, cb)` - Load records into DynamoDB tables from JSON files in the given directory.

__Schema Example__
```js
{
    Users: {
        name: "Users",
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
    }
}
```

### Tables

```js
var schema = dynq.connect("us-east-1").schema();

schema.load(/PREFIX_.*/i, function(err) {
    var table = schema.tables["PREFIX_Users"];
    table.foo(cb => { });
});
```

__Table-Level Members__
* `table.name` - The name of the table.
* `table.schema` - The schema to which this table belongs.
* `table.description` - The metadata from `schema.describeTable(name)`.
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
* `edit.change(values)` - Field values to be changed/overwritten.
* `edit.add(values)` - Field values to be added.  If numeric, this means atomic addition/subtraction.  If a set, this means addition to the set.
* `edit.remove(values)` - Field values to be removed.  If a set, this means removal from the set.
* `edit.conditions(expected)` - Values expected to be found in the record.  If not matched, the operation fails.
* `edit.select(select)` - A list of fields to select from the record.
* `edit.update(cb)` - Updates the record.  If a record does not exist, the operation fails.
* `edit.upsert(cb)` - Upserts the record.  If a record does not exists, one is created.
* `edit.debug(cb)` - Write the JSON of the query and return it if a cb is supplied.

__File Methods__
* `table.backup(filepath, cb)` - Save the contents of the DynamoDB table to a file.
* `table.restore(filepath, cb)` - Load the contents of a file to the DynamoDB table.

### Connection Members

* `cxn.distributeReads` – If multiple masters are specified, each read is dispatched to a randomly selected source.
* `cxn.destinations` – An array of service interfaces used in a multi-master configuration.
* `cxn.debug` –  Outputs all connection operations to the logger.
* `cxn.addRegion(region)` – Adds an additional region to the `destinations` array after construction.

### Key-Value Store Methods

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

### DynamoDB Native Operations

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