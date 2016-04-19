var uuid = require("node-uuid");

module.exports = options => definition => function(table) {
    
    this.test = (cb) => {
        cb(null, table.name);
    };
    
};