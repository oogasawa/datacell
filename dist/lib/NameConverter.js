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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NameConverter = void 0;
const sprintf_js_1 = require("sprintf-js");
const streamlib = __importStar(require("datacell-streamlib"));
const log4js = __importStar(require("log4js"));
const logger = log4js.getLogger();
class NameConverter {
    constructor() {
        this.alnumPattern = /^[A-Za-z0-9_ ]+$/;
        this.prefixPattern = /[A-Za-z0-9_]+/;
        this.tableNamePattern = /^([A-Z0-9_]+?)__([A-Z0-9_]+)$/;
        this.MAX_LENGTH_OF_PREFIX = 20;
        this.NUMBER_OF_DIGITS = 5;
        this.NAME_PREFIX = "NONALNUM";
        this.managementTableNames = [
            "ORIGINAL_NAME__INTERNAL_NAME",
            "INTERNAL_NAME__ORIGINAL_NAME",
            "INTERNAL_NAME__MAX_COUNT"
        ];
        // nothing to do.
    }
    init(store) {
        return __awaiter(this, void 0, void 0, function* () {
            this.store = store;
            yield this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
            yield this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
            yield this.store._createTable("INTERNAL_NAME__MAX_COUNT");
            yield this.setInternalName("ORIGINAL_NAME", "ORIGINAL_NAME");
            yield this.setInternalName("INTERNAL_NAME", "INTERNAL_NAME");
            yield this.setInternalName("MAX_COUNT", "MAX_COUNT");
        });
    }
    /** Set a data cell store to this object and creates management tables.
     *
     * @category Store Access
     *
     * @param store
     */
    setDataCellStore(store) {
        return __awaiter(this, void 0, void 0, function* () {
            this.store = store;
            yield this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
            yield this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
            yield this.store._createTable("INTERNAL_NAME__MAX_COUNT");
        });
    }
    /** Tests whether the given internal name is stored in the NameConverter store.
     *
     * @category Store Access
     * @param internalName
     */
    hasOriginalName(internalName) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield streamlib.streamToArray(yield this.store._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName));
            return result.length > 0;
        });
    }
    /** Returns original name stored in the management table.
     *
     * @category Store Access
     * @param internalName
     */
    getOriginalName(internalName) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield streamlib.streamToArray(yield this.store._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName));
            return result[0];
        });
    }
    /** Returns the internal name corresponds to the given orig name.
     *
     * If the internal name exists in the datacell store,
     * this method returns the name.
     * Otherwise, this method creates a new internal name
     * and returns it.
     *
     * @category Store Access
     * @param origName  - an original name.
     * @return internal name.
     */
    getInternalName(origName) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield streamlib.streamToArray(yield this.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origName));
            if (result.length >= 1) {
                return result[0];
            }
            else {
                const internalName = yield this._makeInternalName(origName);
                yield this.setInternalName(origName, internalName);
                return internalName;
            }
        });
    }
    /** Stores a internalName / originalName pair to the database.
     *
     * @category Store Access
     * @param origName
     * @param internalName
     */
    setInternalName(origName, internalName) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.store._putRowWithReplacingValue("INTERNAL_NAME__ORIGINAL_NAME", internalName, origName);
            yield this.store._putRowWithReplacingValue("ORIGINAL_NAME__INTERNAL_NAME", origName, internalName);
        });
    }
    /** Returns internal name string corresponding to the given origName.
     *
     * This method access to the management tables
     * by calling getCount() method which return counter of the prefix.
     *
     * @category Store Acess
     * @param origName - an original name.
     * @return generated internal name
     */
    _makeInternalName(origName) {
        return __awaiter(this, void 0, void 0, function* () {
            let internalName;
            let prefix;
            let counter = 0;
            const m = this.alnumPattern.exec(origName);
            if (m !== null) { // match to the alnum pattern.
                if (origName.length <= this.MAX_LENGTH_OF_PREFIX) {
                    internalName = origName.split(" ").join("_").toUpperCase();
                }
                else {
                    prefix = origName.substring(0, this.MAX_LENGTH_OF_PREFIX);
                    prefix = prefix.split(" ").join("_").toUpperCase(); // replace all " " with "_".
                    counter = yield this.getCount(prefix);
                    internalName = prefix + sprintf_js_1.sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
                    yield this.setCount(prefix, counter);
                }
            }
            else { // do not match the alnum pattern.
                prefix = this.NAME_PREFIX;
                counter = yield this.getCount(prefix);
                internalName = prefix + sprintf_js_1.sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
                yield this.setCount(prefix, counter);
            }
            // logger.debug("NameConverter::_makeInternalName() : origName = " + origName);
            // logger.debug("NameConverter::_makeInternalName() : internalName = " + internalName);
            return internalName;
        });
    }
    /** Returns the count of given prefix.
     *
     * @category Store Access
     * @param prefix
     */
    getCount(prefix) {
        return __awaiter(this, void 0, void 0, function* () {
            let count = 0;
            const cList = yield streamlib.streamToArray(yield this.store._getValues("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix));
            if (cList.length > 0) {
                count = parseInt(cList[0], 10);
            }
            else {
                count = 0;
            }
            return count;
        });
    }
    /** Set count of given prefix.
     *
     * @category Store Access
     * @param prefix
     * @param count
     */
    setCount(prefix, count) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.store._putRowWithReplacingValue("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix, `${count}`);
        });
    }
    /** Returns a table name (a pair of internal names joined with "__").
     *
     * This method use the management table for getting internal names.
     *
     * @category Store Access
     * @param category
     * @param predicate
     */
    makeTableName(category, predicate) {
        return __awaiter(this, void 0, void 0, function* () {
            let c = yield this.getInternalName(category);
            let p = yield this.getInternalName(predicate);
            return c + "__" + p;
        });
    }
    /** Converts a table name to a pair of category name and predicate.
     *
     * This method do not access to the management tables.
     *
     * @param tableName - a table name.
     * @return a pair of internal names.
     * If the given string do not match the table name pattern,
     * this method returns a zero length array (that is, <code>[]</code>).
     */
    parseTableName(tableName) {
        const result = [];
        const m = this.tableNamePattern.exec(tableName);
        if (m !== null) {
            result.push(m[1].toUpperCase());
            result.push(m[2].toUpperCase());
        }
        return result;
    }
    /** Check if the given table name string matches the table name pattern.
     *
     * This method do not access to the management tables.
     *
     * @param tableName
     */
    isValidTableName(tableName) {
        let m = this.tableNamePattern.exec(tableName);
        return m !== null;
    }
    _report(dest) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._reportTable(dest, "ORIGINAL_NAME__INTERNAL_NAME");
            yield this._reportTable(dest, "INTERNAL_NAME__ORIGINAL_NAME");
            yield this._reportTable(dest, "INTERNAL_NAME__MAX_COUNT");
        });
    }
    _reportTable(dest, tableName) {
        var e_1, _a;
        return __awaiter(this, void 0, void 0, function* () {
            // "ORIGINAL_NAME__INTERNAL_NAME",
            // "INTERNAL_NAME__ORIGINAL_NAME",
            // "INTERNAL_NAME__MAX_COUNT"
            this._print(dest, "# " + tableName);
            let r_stream = yield this.store._getAllRows(tableName);
            let line = null;
            try {
                for (var r_stream_1 = __asyncValues(r_stream), r_stream_1_1; r_stream_1_1 = yield r_stream_1.next(), !r_stream_1_1.done;) {
                    line = r_stream_1_1.value;
                    this._print(dest, JSON.stringify(line));
                }
            }
            catch (e_1_1) { e_1 = { error: e_1_1 }; }
            finally {
                try {
                    if (r_stream_1_1 && !r_stream_1_1.done && (_a = r_stream_1.return)) yield _a.call(r_stream_1);
                }
                finally { if (e_1) throw e_1.error; }
            }
        });
    }
    /** Print a message to stdout (console.log) or logger (log4j.debug).
     *
     * @param dest
     * @param msg
     */
    _print(dest, msg) {
        switch (dest) {
            case "stdout":
                console.log(msg);
                break;
            case "debug":
                logger.debug(msg);
                break;
            default:
                break;
        }
    }
}
exports.NameConverter = NameConverter;
