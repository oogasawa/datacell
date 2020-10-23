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
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NameConverter = void 0;
var sprintf_js_1 = require("sprintf-js");
var streamlib = __importStar(require("datacell-streamlib"));
var log4js = __importStar(require("log4js"));
var logger = log4js.getLogger();
var NameConverter = /** @class */ (function () {
    function NameConverter() {
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
    NameConverter.prototype.init = function (store) {
        this.store = store;
        this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
        this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
        this.store._createTable("INTERNAL_NAME__MAX_COUNT");
    };
    /** Set a data cell store to this object and creates management tables.
     *
     * @category Store Access
     *
     * @param store
     */
    NameConverter.prototype.setDataCellStore = function (store) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        this.store = store;
                        return [4 /*yield*/, this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME")];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME")];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, this.store._createTable("INTERNAL_NAME__MAX_COUNT")];
                    case 3:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /** Tests whether the given internal name is stored in the NameConverter store.
     *
     * @category Store Access
     * @param internalName
     */
    NameConverter.prototype.hasOriginalName = function (internalName) {
        return __awaiter(this, void 0, void 0, function () {
            var result, _a, _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        _b = (_a = streamlib).streamToArray;
                        return [4 /*yield*/, this.store._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName)];
                    case 1: return [4 /*yield*/, _b.apply(_a, [_c.sent()])];
                    case 2:
                        result = _c.sent();
                        return [2 /*return*/, result.length > 0];
                }
            });
        });
    };
    /** Returns original name stored in the management table.
     *
     * @category Store Access
     * @param internalName
     */
    NameConverter.prototype.getOriginalName = function (internalName) {
        return __awaiter(this, void 0, void 0, function () {
            var result, _a, _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        _b = (_a = streamlib).streamToArray;
                        return [4 /*yield*/, this.store._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName)];
                    case 1: return [4 /*yield*/, _b.apply(_a, [_c.sent()])];
                    case 2:
                        result = _c.sent();
                        return [2 /*return*/, result[0]];
                }
            });
        });
    };
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
    NameConverter.prototype.getInternalName = function (origName) {
        return __awaiter(this, void 0, void 0, function () {
            var result, _a, _b, internalName;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        _b = (_a = streamlib).streamToArray;
                        return [4 /*yield*/, this.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origName)];
                    case 1: return [4 /*yield*/, _b.apply(_a, [_c.sent()])];
                    case 2:
                        result = _c.sent();
                        if (!(result.length >= 1)) return [3 /*break*/, 3];
                        return [2 /*return*/, result[0]];
                    case 3: return [4 /*yield*/, this._makeInternalName(origName)];
                    case 4:
                        internalName = _c.sent();
                        return [4 /*yield*/, this.setInternalName(origName, internalName)];
                    case 5:
                        _c.sent();
                        return [2 /*return*/, internalName];
                }
            });
        });
    };
    /** Stores a internalName / originalName pair to the database.
     *
     * @category Store Access
     * @param origName
     * @param internalName
     */
    NameConverter.prototype.setInternalName = function (origName, internalName) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.store._putRowWithReplacingValue("INTERNAL_NAME__ORIGINAL_NAME", internalName, origName)];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, this.store._putRowWithReplacingValue("ORIGINAL_NAME__INTERNAL_NAME", origName, internalName)];
                    case 2:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /** Returns internal name string corresponding to the given origName.
     *
     * This method access to the management tables
     * by calling getCount() method which return counter of the prefix.
     *
     * @category Store Acess
     * @param origName - an original name.
     * @return generated internal name
     */
    NameConverter.prototype._makeInternalName = function (origName) {
        return __awaiter(this, void 0, void 0, function () {
            var internalName, prefix, counter, m;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        counter = 0;
                        m = this.alnumPattern.exec(origName);
                        if (!(m !== null)) return [3 /*break*/, 5];
                        if (!(origName.length <= this.MAX_LENGTH_OF_PREFIX)) return [3 /*break*/, 1];
                        internalName = origName.split(" ").join("_").toUpperCase();
                        return [3 /*break*/, 4];
                    case 1:
                        prefix = origName.substring(0, this.MAX_LENGTH_OF_PREFIX);
                        prefix = prefix.split(" ").join("_").toUpperCase(); // replace all " " with "_".
                        return [4 /*yield*/, this.getCount(prefix)];
                    case 2:
                        counter = _a.sent();
                        internalName = prefix + sprintf_js_1.sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
                        return [4 /*yield*/, this.setCount(prefix, counter)];
                    case 3:
                        _a.sent();
                        _a.label = 4;
                    case 4: return [3 /*break*/, 8];
                    case 5:
                        prefix = this.NAME_PREFIX;
                        return [4 /*yield*/, this.getCount(prefix)];
                    case 6:
                        counter = _a.sent();
                        internalName = prefix + sprintf_js_1.sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
                        return [4 /*yield*/, this.setCount(prefix, counter)];
                    case 7:
                        _a.sent();
                        _a.label = 8;
                    case 8:
                        logger.debug("NameConverter::_makeInternalName() : origName = " + origName);
                        logger.debug("NameConverter::_makeInternalName() : internalName = " + internalName);
                        return [2 /*return*/, internalName];
                }
            });
        });
    };
    /** Returns the count of given prefix.
     *
     * @category Store Access
     * @param prefix
     */
    NameConverter.prototype.getCount = function (prefix) {
        return __awaiter(this, void 0, void 0, function () {
            var count, cList, _a, _b;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        count = 0;
                        _b = (_a = streamlib).streamToArray;
                        return [4 /*yield*/, this.store._getValues("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix)];
                    case 1: return [4 /*yield*/, _b.apply(_a, [_c.sent()])];
                    case 2:
                        cList = _c.sent();
                        if (cList.length > 0) {
                            count = parseInt(cList[0], 10);
                        }
                        return [2 /*return*/, count];
                }
            });
        });
    };
    /** Set count of given prefix.
     *
     * @category Store Access
     * @param prefix
     * @param count
     */
    NameConverter.prototype.setCount = function (prefix, count) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.store._putRowWithReplacingValue("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix, "" + count)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /** Returns a table name (a pair of internal names joined with "__").
     *
     * This method use the management table for getting internal names.
     *
     * @category Store Access
     * @param category
     * @param predicate
     */
    NameConverter.prototype.makeTableName = function (category, predicate) {
        return __awaiter(this, void 0, void 0, function () {
            var c, p;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getInternalName(category)];
                    case 1:
                        c = _a.sent();
                        return [4 /*yield*/, this.getInternalName(predicate)];
                    case 2:
                        p = _a.sent();
                        return [2 /*return*/, c + "__" + p];
                }
            });
        });
    };
    /** Converts a table name to a pair of category name and predicate.
     *
     * This method do not access to the management tables.
     *
     * @param tableName - a table name.
     * @return a pair of internal names.
     * If the given string do not match the table name pattern,
     * this method returns a zero length array (that is, <code>[]</code>).
     */
    NameConverter.prototype.parseTableName = function (tableName) {
        var result = [];
        var m = this.tableNamePattern.exec(tableName);
        if (m !== null) {
            result.push(m[1].toUpperCase());
            result.push(m[2].toUpperCase());
        }
        return result;
    };
    /** Check if the given table name string matches the table name pattern.
     *
     * This method do not access to the management tables.
     *
     * @param tableName
     */
    NameConverter.prototype.isValidTableName = function (tableName) {
        var m = this.tableNamePattern.exec(tableName);
        return m !== null;
    };
    return NameConverter;
}());
exports.NameConverter = NameConverter;
