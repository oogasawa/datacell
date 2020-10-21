"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __values = (this && this.__values) || function(o) {
    var s = typeof Symbol === "function" && Symbol.iterator, m = s && o[s], i = 0;
    if (m) return m.call(o);
    if (o && typeof o.length === "number") return {
        next: function () {
            if (o && i >= o.length) o = void 0;
            return { value: o && o[i++], done: !o };
        }
    };
    throw new TypeError(s ? "Object is not iterable." : "Symbol.iterator is not defined.");
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.MemDB = void 0;
var datacell_collections_1 = require("datacell-collections");
var typescriptcollectionsframework_1 = require("typescriptcollectionsframework");
var AbstractDB_1 = require("./AbstractDB");
var DataCell_1 = require("./DataCell");
var stream_1 = require("stream");
var log4js = __importStar(require("log4js"));
var logger = log4js.getLogger();
logger.level = "debug";
var MemDB = /** @class */ (function (_super) {
    __extends(MemDB, _super);
    // nameConverter: NameConverter;
    function MemDB() {
        var _this = _super.call(this) || this;
        _this.tables = new typescriptcollectionsframework_1.HashMap();
        _this.nameConverter.init(_this);
        return _this;
    }
    MemDB.prototype.close = function () {
        // nothing to do.
    };
    /** @inheritdoc */
    MemDB.prototype._createTable = function (tableName) {
        if (this._hasTable(tableName) === true) {
            return null;
        }
        var kvMap = new datacell_collections_1.DuplicatedKeyUniqueValueHashMap();
        this.tables.put(tableName, kvMap);
        return tableName;
    };
    /** @inheritdoc */
    MemDB.prototype._hasTable = function (tableName) {
        return this.tables.containsKey(tableName);
    };
    /** @inheritdoc */
    MemDB.prototype.hasTable = function (cond) {
        var tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);
        return this._hasTable(tableName);
    };
    /** @inheritdoc */
    MemDB.prototype._deleteTable = function (tableName) {
        if (this._hasTable(tableName) === true) {
            this.tables.remove(tableName);
            return tableName;
        }
        else {
            return null;
        }
    };
    /** @inheritdoc */
    MemDB.prototype.deleteAllTables = function () {
        var e_1, _a;
        var ks = this.tables.keySet();
        try {
            for (var ks_1 = __values(ks), ks_1_1 = ks_1.next(); !ks_1_1.done; ks_1_1 = ks_1.next()) {
                var k = ks_1_1.value;
                this._deleteTable(k);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (ks_1_1 && !ks_1_1.done && (_a = ks_1.return)) _a.call(ks_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
    };
    /** @inheritdoc */
    MemDB.prototype.getAllTablesIncludingManagementTables = function () {
        var result = [];
        // logger.debug("MemDB::getAllTables(): " + this.tables.keySet());
        for (var iter = this.tables.keySet().iterator(); iter.hasNext();) {
            var name = iter.next();
            result.push(name);
        }
        return result;
    };
    /** @inheritdoc */
    MemDB.prototype._getIDs = function (tableName) {
        var result = [];
        var keys = this.tables.get(tableName).keySet();
        for (var iter = keys.iterator(); iter.hasNext();) {
            result.push(iter.next()); // This method pushes only "IDs" to the stream.
        }
        result.push(null);
        return result;
    };
    /** @inheritdoc */
    MemDB.prototype._getIDStream = function (tableName) {
        var readable = new stream_1.Readable({ objectMode: true });
        // const result: string[] = [];
        var keys = this.tables.get(tableName).keySet();
        for (var iter = keys.iterator(); iter.hasNext();) {
            readable.push(iter.next()); // This method pushes only "IDs" to the stream.
        }
        readable.push(null);
        return readable;
    };
    /** @inheritdoc */
    MemDB.prototype._getPredicates = function (category, objectID) {
        var e_2, _a;
        var result = [];
        var tables = this.getAllTables();
        try {
            for (var tables_1 = __values(tables), tables_1_1 = tables_1.next(); !tables_1_1.done; tables_1_1 = tables_1.next()) {
                var t = tables_1_1.value;
                var names = this.nameConverter.parseTableName(t);
                if (this.nameConverter.getOriginalName(names[0]) !== category) {
                    continue;
                }
                var predicate = this.nameConverter.getOriginalName(names[1]);
                if (this.tables.get(t).keySet().contains(objectID)) {
                    result.push(predicate);
                }
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (tables_1_1 && !tables_1_1.done && (_a = tables_1.return)) _a.call(tables_1);
            }
            finally { if (e_2) throw e_2.error; }
        }
        return result;
    };
    /** @inheritdoc */
    MemDB.prototype.getPredicates = function (cond) {
        return this._getPredicates(cond.category, cond.objectId);
    };
    /** @inheritdoc */
    MemDB.prototype._getValues = function (tableName, objectID) {
        var result = [];
        if (!this._hasTable(tableName)) {
            return result;
        }
        if (!this._hasID(tableName, objectID)) {
            return result;
        }
        var tset = this.tables.get(tableName).get(objectID);
        for (var iter = tset.iterator(); iter.hasNext();) {
            var value = iter.next();
            result.push(value);
        }
        return result;
    };
    /** @inheritdoc */
    MemDB.prototype.getValues = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getValues(tableName, cond.objectId);
    };
    /** @inheritdoc */
    MemDB.prototype._getRows = function (tableName, objectID) {
        var result = [];
        var names = this.nameConverter.parseTableName(tableName);
        var category = this.nameConverter.getOriginalName(names[0]);
        var predicate = this.nameConverter.getOriginalName(names[1]);
        var tset = this.tables.get(tableName).get(objectID);
        for (var iter = tset.iterator(); iter.hasNext();) {
            var value = iter.next();
            result.push(new DataCell_1.DataCell(category, objectID, predicate, value));
        }
        return result;
    };
    /** @inheritdoc */
    MemDB.prototype.getRows = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getRows(tableName, cond.objectId);
    };
    /** @inheritdoc */
    MemDB.prototype._getAllRows = function (tableName) {
        var result = [];
        var names = this.nameConverter.parseTableName(tableName);
        var category = this.nameConverter.getOriginalName(names[0]);
        var predicate = this.nameConverter.getOriginalName(names[1]);
        var ids = this.tables.get(tableName).keySet();
        for (var iter = ids.iterator(); iter.hasNext();) {
            var objectID = iter.next();
            var tset = this.tables.get(tableName).get(objectID);
            for (var iter2 = tset.iterator(); iter2.hasNext();) {
                var value = iter2.next();
                result.push(new DataCell_1.DataCell(category, objectID, predicate, value));
            }
        }
        return result;
    };
    /** @inheritdoc */
    MemDB.prototype._getStreamOfAllRows = function (tableName) {
        var readable = new stream_1.Readable({ objectMode: true });
        var names = this.nameConverter.parseTableName(tableName);
        var category = this.nameConverter.getOriginalName(names[0]);
        var predicate = this.nameConverter.getOriginalName(names[1]);
        var ids = this.tables.get(tableName).keySet();
        for (var iter = ids.iterator(); iter.hasNext();) {
            var objectID = iter.next();
            var tset = this.tables.get(tableName).get(objectID);
            for (var iter2 = tset.iterator(); iter2.hasNext();) {
                var value = iter2.next();
                readable.push(new DataCell_1.DataCell(category, objectID, predicate, value));
            }
        }
        return readable;
    };
    /** @inheritdoc */
    MemDB.prototype._hasID = function (tableName, objectID) {
        if (!this._hasTable(tableName)) {
            return false;
        }
        var tset = this.tables.get(tableName).get(objectID);
        if (tset !== undefined && tset.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    };
    /** @inheritdoc */
    MemDB.prototype.hasID = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasID(tableName, cond.objectId);
    };
    /** @inheritdoc */
    MemDB.prototype._hasRow = function (tableName, objectID, value) {
        if (!this._hasTable(tableName) || !this._hasID(tableName, objectID)) {
            return false;
        }
        var tset = this.tables.get(tableName).get(objectID);
        for (var iter = tset.iterator(); iter.hasNext();) {
            var v = iter.next();
            if (this._compareObjectValues(value, v)) {
                return true;
            }
        }
        return false;
    };
    /** @inheritdoc */
    MemDB.prototype.hasRow = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasRow(tableName, cond.objectId, cond.value);
    };
    /** @inheritdoc */
    // _compareObjectValues(obj1: any, obj2: any) {
    //     let str1 = JSON.stringify(obj1);
    //     let str2 = JSON.stringify(obj2);
    //     return str1 === str2;
    // }
    /** @inheritdoc */
    MemDB.prototype._addRow = function (tableName, objectID, value) {
        // This method assumes that the table has already existed.
        if (this.tables.get(tableName).containsKey(objectID)) {
            this.tables.get(tableName).get(objectID).add(value);
        }
        else {
            this.tables.get(tableName).put(objectID, value);
        }
    };
    /** @inheritdoc */
    // addRow(cell: DataCell): void {
    //     const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
    //     return this._addRow(tableName, cell.objectId, cell.value);
    // }
    /** @inheritdoc */
    // _putRow(tableName: string, objectID: string, value: any): void {
    //     if (!this._hasTable(tableName)) {
    //         this._createTable(tableName);
    //     }
    //     this._addRow(tableName, objectID, value);
    // }
    /** @inheritdoc */
    MemDB.prototype.putRow = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRow(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    // _putRowIfKeyValuePairIsAbsent(tableName: string, objectID: string, value: any): void {
    //     if (!this._hasTable(tableName)) {
    //         this._createTable(tableName);
    //     }
    //     this._addRow(tableName, objectID, value);
    // }
    /** @inheritdoc */
    MemDB.prototype.putRowIfKeyValuePairIsAbsent = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    // _putRowIfKeyIsAbsent(tableName: string, objectID: string, value: any): void {
    //     if (!this._hasTable(tableName)) {
    //         this._createTable(tableName);
    //     }
    //     if (!this._hasID(tableName, objectID)) {
    //         this._addRow(tableName, objectID, value);
    //     }
    // }
    /** @inheritdoc */
    MemDB.prototype.putRowIfKeyIsAbsent = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    MemDB.prototype._putRowWithReplacingValue = function (tableName, objectID, value) {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }
        if (!this._hasRow(tableName, objectID, value)) {
            this.tables.get(tableName).set(objectID, value);
        }
    };
    /** @inheritdoc */
    // putRowWithReplacingValue(cell: DataCell): void {
    //     const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
    //     return this._putRowWithReplacingValue(tableName, cell.objectId, cell.value);
    // }
    /** @inheritdoc */
    MemDB.prototype.deleteRow = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        this.tables.get(tableName).get(cond.objectId).remove(cond.value);
    };
    // writeToFile(fname: string, tableName?: string): Promise<void> {
    //     if (tableName !== null) {
    //         this._writeToFile(fname, tableName);
    //     }
    //     else {
    //         for (let t of this.getAllTables()) {
    //             this._writeToFile(fname, t);
    //         }
    //     }
    // }
    // /** @inheritdoc */
    // print(): void {
    //     const categories = this.getAllCategories().sort();
    //     // console.log("categories.length: " + categories.length);
    //     for (const c of categories) {
    //         const ids: string[] = this._categoryToObjectIDs(c).sort();
    //         // console.log("  ids.length: " + ids.length);
    //         for (const i of ids) {
    //             const preds: string[] = this._getPredicates(c, i).sort();
    //             // console.log("    preds.length: " + preds.length);
    //             for (const p of preds) {
    //                 const values: any[] = this.getValues(new DataCell(c, i, p, ""));
    //                 // console.log("    values.length: " + values.length);
    //                 for (const v of values) {
    //                     console.log(c + "\t" + i + "\t" + p + "\t" + JSON.stringify(v));
    //                 }
    //             }
    //         }
    //     }
    // }
    MemDB.prototype._categoryToObjectIDs = function (category) {
        var e_3, _a;
        var result = [];
        var objectIDs = new typescriptcollectionsframework_1.TreeSet(new datacell_collections_1.StringComparator());
        // the variable "tables" is an array of table names.
        var tables = this._categoryToTables(category);
        try {
            for (var tables_2 = __values(tables), tables_2_1 = tables_2.next(); !tables_2_1.done; tables_2_1 = tables_2.next()) { // t is a table name.
                var t = tables_2_1.value;
                var ks = this.tables.get(t).keySet();
                for (var iter = ks.iterator(); iter.hasNext();) {
                    objectIDs.add(iter.next());
                }
            }
        }
        catch (e_3_1) { e_3 = { error: e_3_1 }; }
        finally {
            try {
                if (tables_2_1 && !tables_2_1.done && (_a = tables_2.return)) _a.call(tables_2);
            }
            finally { if (e_3) throw e_3.error; }
        }
        for (var iter = objectIDs.iterator(); iter.hasNext();) {
            result.push(iter.next());
        }
        return result;
    };
    return MemDB;
}(AbstractDB_1.AbstractDB));
exports.MemDB = MemDB;
// -----
