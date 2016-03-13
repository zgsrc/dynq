require("sugar");

module.exports = {
    name: "Sessions",
    key: { id: "text", timestamp: "number" }, 
    indices: {
        ByUser: {
            columns: { user: "text", timestamp: "number" },
            project: "KEYS_ONLY"
        },
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