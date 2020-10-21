"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MemDBFactory = void 0;
var MemDB_1 = require("./MemDB");
var MemDBFactory = /** @class */ (function () {
    function MemDBFactory() {
    }
    MemDBFactory.prototype.getInstance = function (dbName) {
        return new MemDB_1.MemDB();
    };
    return MemDBFactory;
}());
exports.MemDBFactory = MemDBFactory;
