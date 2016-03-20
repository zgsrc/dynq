require("sugar");

module.exports = {
    name: "Sessions",
    key: { id: "text", timestamp: "number" }, 
    sorts: {
        ByOther: {
            columns: { other: "number" },
            project: [ "id", "other" ]
        },
        ByThird: {
            columns: { third: "number" },
            project: "ALL"
        },
        ByForth: {
            columns: { forth: "number" }
        }
    },
    indices: {
        ByUser: {
            columns: { user: "text", timestamp: "number" },
            project: [ "user", "timestamp" ] 
        },
        ByTimestamp: {
            columns: { timestamp: "number" },
            project: "KEYS_ONLY"
        }
    }
};