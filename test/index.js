require("sugar");

var chai = require("chai"),
    expect = chai.expect,
    dynq = null;

chai.should();

describe('Connection', function() {
    
    it("ain't broke", function() {
        dynq = require("../index");
        dynq.configFromPath(__dirname + "/../test.json");
    });
    
});