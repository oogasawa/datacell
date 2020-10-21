"use strict";
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
exports.AbstractDB = void 0;
var typescriptcollectionsframework_1 = require("typescriptcollectionsframework");
var DataCell_1 = require("./DataCell");
var NameConverter_1 = require("./NameConverter");
var datacell_collections_1 = require("datacell-collections");
var log4js = __importStar(require("log4js"));
var logger = log4js.getLogger();
logger.level = 'error';
var AbstractDB = /** @class */ (function () {
    function AbstractDB() {
        this.nameConverter = new NameConverter_1.NameConverter();
    }
    /** @inheritdoc */
    AbstractDB.prototype.getNameConverter = function () {
        return this.nameConverter;
    };
    /** @inheritdoc */
    AbstractDB.prototype.createTable = function (cond) {
        var tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);
        return this._createTable(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.hasTable = function (cond) {
        var tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);
        return this._hasTable(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.deleteTable = function (cond) {
        var tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);
        return this._deleteTable(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllTables = function () {
        var e_1, _a;
        var result = [];
        var tables = this.getAllTablesIncludingManagementTables();
        try {
            for (var tables_1 = __values(tables), tables_1_1 = tables_1.next(); !tables_1_1.done; tables_1_1 = tables_1.next()) {
                var t = tables_1_1.value;
                if (!this._isManagementTable(t)) {
                    result.push(t);
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (tables_1_1 && !tables_1_1.done && (_a = tables_1.return)) _a.call(tables_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return result;
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllCategories = function () {
        var e_2, _a;
        var result = [];
        var tset = new typescriptcollectionsframework_1.TreeSet(new datacell_collections_1.StringComparator());
        var tables = this.getAllTables();
        try {
            // logger.debug("AbstractDB::getAllCategories: " + tables);
            // unique the category names.
            for (var tables_2 = __values(tables), tables_2_1 = tables_2.next(); !tables_2_1.done; tables_2_1 = tables_2.next()) {
                var t = tables_2_1.value;
                var names = this.nameConverter.parseTableName(t);
                if (names.length === 2) {
                    tset.add(names[0]);
                }
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (tables_2_1 && !tables_2_1.done && (_a = tables_2.return)) _a.call(tables_2);
            }
            finally { if (e_2) throw e_2.error; }
        }
        for (var iter = tset.iterator(); iter.hasNext();) {
            result.push(this.nameConverter.getOriginalName(iter.next()));
        }
        return result;
    };
    /** @inheritdoc */
    AbstractDB.prototype.getIDs = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getIDs(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getIDStream = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getIDStream(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getPredicates = function (cond) {
        return this._getPredicates(cond.category, cond.objectId);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllPredicates = function (category) {
        var e_3, _a;
        var result = [];
        var tables = this.getAllTables();
        try {
            for (var tables_3 = __values(tables), tables_3_1 = tables_3.next(); !tables_3_1.done; tables_3_1 = tables_3.next()) {
                var t = tables_3_1.value;
                var names = this.nameConverter.parseTableName(t);
                var cat = this.nameConverter.getOriginalName(names[0]);
                var pred = this.nameConverter.getOriginalName(names[1]);
                if (category === cat) {
                    result.push(pred);
                }
            }
        }
        catch (e_3_1) { e_3 = { error: e_3_1 }; }
        finally {
            try {
                if (tables_3_1 && !tables_3_1.done && (_a = tables_3.return)) _a.call(tables_3);
            }
            finally { if (e_3) throw e_3.error; }
        }
        return result;
    };
    /** @inheritdoc */
    AbstractDB.prototype.getValues = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getValues(tableName, cond.objectId);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getRows = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getRows(tableName, cond.objectId);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllRows = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getAllRows(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getStreamOfAllRows = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getStreamOfAllRows(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.hasID = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasID(tableName, cond.objectId);
    };
    /** @inheritdoc */
    AbstractDB.prototype.hasRow = function (cond) {
        var tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasRow(tableName, cond.objectId, cond.value);
    };
    /** @inheritdoc */
    AbstractDB.prototype._compareObjectValues = function (obj1, obj2) {
        var str1 = JSON.stringify(obj1);
        var str2 = JSON.stringify(obj2);
        return str1 === str2;
    };
    /** @inheritdoc */
    AbstractDB.prototype.addRow = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._addRow(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRow = function (tableName, objectID, value) {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }
        this._addRow(tableName, objectID, value);
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRow = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRow(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRowIfKeyValuePairIsAbsent = function (tableName, objectID, value) {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }
        this._addRow(tableName, objectID, value);
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRowIfKeyValuePairIsAbsent = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRowIfKeyIsAbsent = function (tableName, objectID, value) {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }
        if (!this._hasID(tableName, objectID)) {
            this._addRow(tableName, objectID, value);
        }
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRowIfKeyIsAbsent = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value);
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRowWithReplacingValue = function (cell) {
        var tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowWithReplacingValue(tableName, cell.objectId, cell.value);
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
    /** @inheritdoc */
    AbstractDB.prototype.print = function () {
        var e_4, _a, e_5, _b, e_6, _c, e_7, _d;
        var categories = this.getAllCategories().sort();
        try {
            // console.log("categories.length: " + categories.length);
            for (var categories_1 = __values(categories), categories_1_1 = categories_1.next(); !categories_1_1.done; categories_1_1 = categories_1.next()) {
                var c = categories_1_1.value;
                var ids = this._categoryToObjectIDs(c).sort();
                try {
                    // console.log("  ids.length: " + ids.length);
                    for (var ids_1 = (e_5 = void 0, __values(ids)), ids_1_1 = ids_1.next(); !ids_1_1.done; ids_1_1 = ids_1.next()) {
                        var i = ids_1_1.value;
                        var preds = this._getPredicates(c, i).sort();
                        try {
                            // console.log("    preds.length: " + preds.length);
                            for (var preds_1 = (e_6 = void 0, __values(preds)), preds_1_1 = preds_1.next(); !preds_1_1.done; preds_1_1 = preds_1.next()) {
                                var p = preds_1_1.value;
                                var values = this.getValues(new DataCell_1.DataCell(c, i, p, ""));
                                try {
                                    // console.log("    values.length: " + values.length);
                                    for (var values_1 = (e_7 = void 0, __values(values)), values_1_1 = values_1.next(); !values_1_1.done; values_1_1 = values_1.next()) {
                                        var v = values_1_1.value;
                                        console.log(c + "\t" + i + "\t" + p + "\t" + JSON.stringify(v));
                                    }
                                }
                                catch (e_7_1) { e_7 = { error: e_7_1 }; }
                                finally {
                                    try {
                                        if (values_1_1 && !values_1_1.done && (_d = values_1.return)) _d.call(values_1);
                                    }
                                    finally { if (e_7) throw e_7.error; }
                                }
                            }
                        }
                        catch (e_6_1) { e_6 = { error: e_6_1 }; }
                        finally {
                            try {
                                if (preds_1_1 && !preds_1_1.done && (_c = preds_1.return)) _c.call(preds_1);
                            }
                            finally { if (e_6) throw e_6.error; }
                        }
                    }
                }
                catch (e_5_1) { e_5 = { error: e_5_1 }; }
                finally {
                    try {
                        if (ids_1_1 && !ids_1_1.done && (_b = ids_1.return)) _b.call(ids_1);
                    }
                    finally { if (e_5) throw e_5.error; }
                }
            }
        }
        catch (e_4_1) { e_4 = { error: e_4_1 }; }
        finally {
            try {
                if (categories_1_1 && !categories_1_1.done && (_a = categories_1.return)) _a.call(categories_1);
            }
            finally { if (e_4) throw e_4.error; }
        }
    };
    AbstractDB.prototype._categoryToTables = function (category) {
        var e_8, _a;
        var result = [];
        var categoryInternalName = this.nameConverter._makeInternalName(category);
        var tables = this.getAllTables();
        try {
            for (var tables_4 = __values(tables), tables_4_1 = tables_4.next(); !tables_4_1.done; tables_4_1 = tables_4.next()) {
                var t = tables_4_1.value;
                var cps = this.nameConverter.parseTableName(t);
                if (cps[0] === categoryInternalName) {
                    result.push(t);
                }
            }
        }
        catch (e_8_1) { e_8 = { error: e_8_1 }; }
        finally {
            try {
                if (tables_4_1 && !tables_4_1.done && (_a = tables_4.return)) _a.call(tables_4);
            }
            finally { if (e_8) throw e_8.error; }
        }
        return result;
    };
    AbstractDB.prototype._tablesToPredicates = function (tableList) {
        var e_9, _a;
        var result = [];
        try {
            for (var tableList_1 = __values(tableList), tableList_1_1 = tableList_1.next(); !tableList_1_1.done; tableList_1_1 = tableList_1.next()) {
                var t = tableList_1_1.value;
                var cps = this.nameConverter.parseTableName(t);
                result.push(this.nameConverter.getOriginalName(cps[1]));
            }
        }
        catch (e_9_1) { e_9 = { error: e_9_1 }; }
        finally {
            try {
                if (tableList_1_1 && !tableList_1_1.done && (_a = tableList_1.return)) _a.call(tableList_1);
            }
            finally { if (e_9) throw e_9.error; }
        }
        return result;
    };
    /** @inheritdoc */
    AbstractDB.prototype._isManagementTable = function (tableName) {
        return this.nameConverter.managementTableNames.includes(tableName);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getManagementTableNames = function () {
        return this.nameConverter.managementTableNames;
    };
    return AbstractDB;
}());
exports.AbstractDB = AbstractDB;
// -----
