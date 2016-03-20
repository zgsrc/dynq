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
    });
    
    it("can create a connection", function() {
        var config = JSON.parse(fs.readFileSync(__dirname + "/../test.json"));
        cxn = dynq.config(config).connect();
    });
    
    it("can define a schema", function() {
        schema = cxn.schema().define({
            test: {
                name: "TEST_test_table",
                key: { id: "S" }, 
                read: 5,
                write: 5
            }
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
        })
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
        schema.create(function(err) {
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
    
    it("can edit and upsert a record", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "four" }).select("ALL_NEW").upsert(function(err, item) {
            if (err) throw err;
            else item.should.be.ok;
            done();
        });
    });
    
    it("can edit and update a record", function(done) {
        schema.tables.test.edit({ id: "1" }).change({ value: "five" }).update(function(err) {
            if (err) throw err;
            done();
        });
    });
    
    it("can write a record", function(done) {
        schema.tables.test.write({ id: "1", value: "six" }, function(err) {
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
        schema.tables.test.writeAll((1).upto(100).map((i) => { 
            return { id: i.toString() }; 
        }), function(err) {
            if (err) throw err;
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
        schema.tables.test.getAll((1).upto(100).map((i) => { 
            return { id: i.toString() }; 
        }), function(err, items) {
            if (err) throw err;
            else items.length.should.equal(100);
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
        schema.tables.test.deleteAll((1).upto(100).map((i) => { 
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
    
    it("can restore records", function(done) {
        this.timeout(60000);
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
    
    it("can project records", function(done) {
        schema.tables.test.scan().select().all(function(err, results) {
            if (err) throw err;
            else results.items.length.should.equal(100);
            done();
        });
    });
    
    it("can require a schema", function(done) {
        this.timeout(120000);
        schema.require(__dirname + "/../examples/require").create(function(err) {
            if (err) throw err;
            else done();
        });
    });
    
    it("can drop a schema", function(done) {
        this.timeout(120000);
        schema.drop(function(err) {
            if (err) throw err;
            else done();
        });
    });
    
});