var uuid = require("node-uuid");

module.exports = options => definition => {
    
    definition.indices.ByCode = {
        columns: { code: "number" }
    };
    
    return function(table) {
        this.test = (cb) => {
            cb(null, table.name);
        };
    };
};