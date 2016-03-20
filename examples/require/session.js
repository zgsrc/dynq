require("sugar");

module.exports = {
    name: "Sessions",
    key: { id: "text", timestamp: "number" }, 
    sorts: {
        ByOther: {
            columns: { other: "number" }
        }
    },
    indices: {
        ByUser: {
            columns: { user: "text", timestamp: "number" },
            project: "KEYS_ONLY"
        },
        ByTimestamp: {
            columns: { timestamp: "number" },
            project: "KEYS_ONLY"
        }
    }
};