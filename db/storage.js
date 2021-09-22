const MemoryDBAdapter = require("../controllers/memory-db-adapter");
const PsqlDBAdapter = require("../controllers/psql-db-adapter");


if (process.env.NODE_ENV !== "production") {
    require("dotenv").config();
}

let DBAdapter = null;
if (process.env.APP_DB_PSQL_URL) {
    console.log("Test Adserver using PSQL storage...")
    DBAdapter = new PsqlDBAdapter();
} else {
    console.log("Test Adserver using MEMORY storage...")
    DBAdapter = new MemoryDBAdapter();
}

module.exports = DBAdapter;