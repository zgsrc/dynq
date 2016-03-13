require("sugar");

module.exports = {
    name: "Users",
    key: { id: "text" }, 
    indices: {
        ByTimestamp: {
            columns: { timestamp: "number" },
            project: "KEYS_ONLY"
        }
    },
    methods: function(table) {
        this.sample = function(cb) {
            cb();
        };
    }
};