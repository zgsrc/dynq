require("sugar");

var chai = require("chai"),
    expect = chai.expect,
    dynq = null,
    cxn = null,
    schema = null;

chai.should();

describe('Module', function() {
    
    this.timeout(10000);
    
    it("ain't broke", function() {
        dynq = require("../index");
        dynq.configFromPath(__dirname + "/../test.json");
    });
    
    it("can create a connection", function() {
        cxn = dynq.connect();
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
    })
    
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
    
    it("can drop a schema", function(done) {
        this.timeout(120000);
        schema.drop(function(err) {
            if (err) throw err;
            else done();
        });
    });
    
});