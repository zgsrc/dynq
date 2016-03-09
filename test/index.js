require("sugar");

var dynq = null;

describe('Connection', function() {
    
    it("ain't broke", function() {
        dynq = require("../index");
        dynq.configFromPath(__dirname + "/../test.json");
    });
    
});