require("sugar");

module.exports = function(options) {
    return {
        name: "Users",
        key: { id: "text" }, 
        indices: {
            ByTimestamp: {
                columns: { timestamp: "number" }
            }
        },
        methods: function(table) {
            this.sample = function(cb) {
                cb();
            };
            
            this.nonFunction = "asdf";
        }
    };
};