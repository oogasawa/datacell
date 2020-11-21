"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MemDBFactory = void 0;
const MemDB_1 = require("./MemDB");
class MemDBFactory {
    getInstance(dbName) {
        return new MemDB_1.MemDB();
    }
}
exports.MemDBFactory = MemDBFactory;
