require("sugar");

var fs = require("fs"),
    chai = require("chai"),
    async = require("async"),
    expect = chai.expect,
    dynq = null,
    cxn = null,
    schema = null;

chai.should();

describe('Module', function() {
    
    this.timeout(10000);
    
    it("ain't broke", function() {
        dynq = require("../index");
        dynq.debug = true;
        dynq.logger = () => { };
    });
    
    it("can create a connection", function() {
        var config = JSON.parse(fs.readFileSync(__dirname + "/../test.json"));
        cxn = dynq.config(config).connect("us-east-1", true);
        
        cxn.destinations = [ ];
        cxn.addRegion();
        
        cxn.destinations = [ ];
        cxn = dynq.connect();
        
        cxn.debug = true;
    });
    
    it("can access the throughput handler", function() {
        dynq.throughputHandler(null, "table", "index");
    });
    
    it("can define a schema", function() {
        schema = cxn.schema().define({
            test: {
                name: "test_table",
                key: { id: "S" }, 
                read: 5,
                write: 5
            }
        });
    });
    
    it("can fail to create table without name", function(done) {
        schema.createTable({ }, function(err) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can fail to create table without primary key", function(done) {
        schema.createTable({ name: "whatever" }, function(err) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can list some tables", function(done) {
        schema.listSomeTables(function(err, result) {
            if (err) throw err;
            else {
                expect(result.tables).to.be.an("array");
                if (result.last) expect(result.last).to.be.a("string");
                done();
            }
        });
    });
    
    it("can list all tables", function(done) {
        schema.listAllTables(function(err, tables) {
            if (err) throw err;
            else expect(tables).to.be.an("array");
            done();
        })
    });
    
    it("can create a schema", function(done) {
        this.timeout(120000);
        schema.create({ 
            minReadCapacity: 5, 
            minWriteCapacity: 5, 
            prefix: "TEST_" 
        }, function(err) {
            if (err) throw err;
            else done();
        });
    });
    
    it("has a test table", function() {
        expect(schema.tables.test).to.be.ok;
    });
    
    it("can insert a record", function(done) {
        schema.tables.test.insert({ id: "1", value: "one" }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("cannot re-insert a record", function(done) {
        schema.tables.test.insert({ id: "1", value: "one" }, function(err) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can upsert a record", function(done) {
        schema.tables.test.upsert({ id: "1", value: "two" }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can update a record", function(done) {
        schema.tables.test.update({ id: "1", value: "three" }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can debug edit operation", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "four", some: null }).select("ALL_NEW").debug(function(err, item) {
            if (err) throw err;
            else item.should.be.ok;
            done();
        });
    });
    
    it("cannot edit with false conditions", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "four" }).conditions({ value: "four" }).update(function(err, item) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can edit with true conditions", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "five" }).conditions({ value: "three" }).select("ALL_NEW").update(function(err, item) {
            if (err) throw err;
            else item.should.be.ok;
            done();
        });
    });
    
    it("can edit and upsert a record", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "four" }).add({ set: [ 1, 2 ] }).select("ALL_NEW").upsert(function(err, item) {
            if (err) throw err;
            else item.should.be.ok;
            done();
        });
    });
    
    it("can edit and update a record", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "five" }).remove({ set: [ 2 ] }).update(function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can write a record", function(done) {
        schema.tables.test.write({ 
            id: "1", 
            value: "six",
            x1: 1,
            x2: true,
            x3: [ "asdf", "zxcv" ],
            x4: [ 1, 2, 3 ],
            x5: new Buffer(3),
            x6: [ new Buffer(2), new Buffer(3) ],
            x7: { 
                x: 3, 
                y: "asdf", 
                z: [ 3, "hello", null ], 
                n1: [ 1, 2, 3], 
                s1: [ "hello", "goodbye" ], 
                b1: [ new Buffer(1), { list: [ 1 ] } ], 
                b2: new Buffer(1), 
                d1: new Date(), 
                ae: [ ] 
            },
            x8: [ 
                { x: 3, y: "asdf" }, 
                { x: 3, y: "asdf" }, 
                [ 1, 2, 3 ], 
                [ "hello", "goodbye" ], 
                [ new Buffer(1) ], 
                [ "asdf", 1, false, { okay: 1 } ],
                new Buffer(1),
                new Date(),
                [ ]
            ],
            x9: null,
            x10: new Date()
        }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can confirm a record exists", function(done) {
        schema.tables.test.exists({ id: "1" }, function(err, exists) {
            if (err) throw err;
            else expect(exists).to.be.ok;
            done();
        });
    });
    
    it("can get a record", function(done) {
        schema.tables.test.get({ id: "1" }, function(err, item) {
            if (err) throw err;
            else expect(item).to.be.an("object");
            done();
        });
    });
    
    it("can get a record by value", function(done) {
        schema.tables.test.get("1", function(err, item) {
            if (err) throw err;
            else expect(item).to.be.an("object");
            done();
        });
    });
    
    it("can get part of a record", function(done) {
        schema.tables.test.getPart({ id: "1" }, [ "id" ], function(err, item) {
            if (err) throw err;
            else {
                expect(item).to.be.an("object");
                Object.keys(item).length.should.equal(1);
            }
            done();
        });
    });
    
    it("cannot conditionally delete a record with incorrect values", function(done) {
        schema.tables.test.deleteIf({ id: "1" }, { value: "one" }, function(err, item) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can conditionally delete a record with correct values", function(done) {
        schema.tables.test.deleteIf({ id: "1" }, { value: "six" }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can delete a record twice", function(done) {
        schema.tables.test.delete({ id: "1" }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can over-write a record", function(done) {
        schema.tables.test.write({ id: "1", value: "one" }, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can delete a record", function(done) {
        schema.tables.test.delete({ id: "1" }, function(err) {
            done();
        });
    });
    
    it("can confirm a record does not exists", function(done) {
        schema.tables.test.exists({ id: "1" }, function(err, exists) {
            if (err) throw err;
            else expect(exists).to.be.not.ok;
            done();
        });
    });
    
    it("can write multiple records", function(done) {
        schema.tables.test.writeAll((1).upto(105).map((i) => { 
            return { id: i.toString() }; 
        }), function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("cannot backup records to non-existant directory", function(done) {
        schema.backup(__dirname + "/dne", function(err) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can backup records", function(done) {
        schema.backup(__dirname, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can get multiple records", function(done) {
        schema.tables.test.getAll((1).upto(105).map((i) => { return { id: i.toString() }; }), [ "id" ], function(err, items) {
            if (err) throw err;
            else items.length.should.equal(105);
            done();
        });
    });
    
    it("can get many records with more than 100 items", function(done) {
        var options = { };
        options[schema.tables.test.name] = {
            keys: (1).upto(105).map((i) => { return { id: i.toString() }; }),
            select: [ "id" ]
        };
        
        cxn.getMany(options, function(err, results) {
            if (err) throw err;
            else results[schema.tables.test.name].length.should.equal(105);
            done();
        });
    });
    
    it("can get many records fewer than 100 records", function(done) {
        var options = { };
        options[schema.tables.test.name] = {
            keys: (1).upto(99).map((i) => { return { id: i.toString() }; }),
            select: [ "id" ]
        };
        
        cxn.getMany(options, function(err, results) {
            if (err) throw err;
            else results[schema.tables.test.name].length.should.equal(99);
            done();
        });
    });
    
    it("can select and delete multiple records", function(done) {
        schema.tables.test.scan().delete().all(function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can delete multiple records", function(done) {
        schema.tables.test.deleteAll((1).upto(105).map((i) => { 
            return { id: i.toString() }; 
        }), function(err, items) {
            if (err) throw err;
            done();
        });
    });
    
    it("has no records", function(done) {
        schema.tables.test.scan().all(function(err, items) {
            if (err) throw err;
            else items.count.should.equal(0);
            done();
        })
    });
    
    it("cannot restore records with non-existant directory", function(done) {
        schema.restore(__dirname + "/dne", function(err) {
            err.should.be.ok;
            done();
        });
    });
    
    it("can restore records", function(done) {
        this.timeout(120000);
        schema.restore(__dirname, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can remove backup files", function(done) {
        schema.removeBackupFiles(__dirname, function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can get first record", function(done) {
        schema.tables.test.query().conditions({ id: [ "EQ", "1" ] }).backwards().select([ "id" ]).first(function(err, result) {
            if (err) throw err;
            else result.should.be.ok;
            done();
        });
    });
    
    it("can project first record", function(done) {
        schema.tables.test.query().conditions({ id: [ "EQ", "1" ] }).backwards().select().first(function(err, result) {
            if (err) throw err;
            else result.should.be.ok;
            done();
        });
    });
    
    it("can delete first record", function(done) {
        schema.tables.test.query().conditions({ id: [ "EQ", "1" ] }).backwards().delete().first(function(err, result) {
            if (err) throw err;
            done();
        });
    });
    
    it("can segment a scan", function(done) {
        schema.tables.test.scan().segment(1, 2).select("id").all(function(err, results) {
            if (err) throw err;
            else results.items.should.be.ok;
            done();
        });
    });
    
    it("can filter a scan", function(done) {
        schema.tables.test.scan().filter({ id: [ "EQ", "1" ], value: [ "EQ", "one" ] }).or().all(function(err, results) {
            if (err) throw err;
            else results.items.should.be.ok;
            done();
        });
    });
    
    it("can apply expressions to a scan", function(done) {
        schema.tables.test.scan().alias("i", "id").parameter("v", "1").filter("#i = :v").all(function(err, results) {
            if (err) throw err;
            else results.items.should.be.ok;
            done();
        });
    });
    
    it("can get a limited number of records", function(done) {
        schema.tables.test.scan().limit(20).select("ALL_ATTRIBUTES").all(function(err, results) {
            if (err) throw err;
            else results.items.length.should.equal(20);
            done();
        });
    });
    
    it("can debug scan", function(done) {
        schema.tables.test.scan().select().debug(function(err, result) {
            if (err) throw err;
            else result.should.be.ok;
            done();
        });
    });
    
    it("can project records", function(done) {
        schema.tables.test.scan().select().all(function(err, results) {
            if (err) throw err;
            else results.items.length.should.equal(104);
            done();
        });
    });
    
    it("can change throughput on table", function(done) {
        this.timeout(30000);
        schema.tables.test.changeThroughput(6, 6, function(err) {
            if (err) throw err;
            else done();
        });
    });
    
    it("can require a schema", function(done) {
        this.timeout(180000);
        schema.require(__dirname + "/../examples/require").require(__dirname + "/../examples/require/user.js").create(function(err) {
            if (err) throw err;
            else done();
        });
    });
    
    it("can query an index", function(done) {
        schema.tables.user.index("ByTimestamp", { timestamp: [ "EQ", 1234 ] }).page(function(err, items) {
            if (err) throw err;
            done();
        });
    });
    
    it("can call a table method", function(done) {
        schema.tables.user.sample(done);
    });
    
    it("can change throughput on index", function(done) {
        this.timeout(30000);
        schema.tables.user.changeIndexThroughput("ByTimestamp", 6, 6, function(err) {
            if (err) throw err;
            else done();
        });
    });
    
    it("can factor throughput", function(done) {
        this.timeout(30000);
        schema.tables.session.factorThroughput(1.1, function(err) {
            if (err) throw err;
            else done();
        })
    });
    
    it("can drop a table", function(done) {
        this.timeout(30000);
        schema.tables.test.drop(function(err) {
            if (err) throw err;
            done();
        })
    });
    
    it("can drop a schema", function(done) {
        this.timeout(180000);
        schema.drop(function(err) {
            if (err) throw err;
            else done();
        });
    });
    
});