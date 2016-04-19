var uuid = require("node-uuid");

module.exports = options => defintions => function(table) {
    
    this.create = (item, cb) => {
        if (!item.id) item.id = uuid();
        item.created = Date.create();
        item.timestamp = Date.create().getTime();
        table.insert(item, cb);
    };
    
    this.modify = (item, cb) => {
        if (!item.id) item.id = uuid();
        item.modified = Date.create();
        item.timestamp = Date.create().getTime();
        table.insert(item, cb);
    };
    
};