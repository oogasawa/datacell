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
var AbstractDB_1 = require("./AbstractDB");
var DataCell_1 = require("./DataCell");
var stream_1 = require("stream");
var streamlib = __importStar(require("datacell-streamlib"));
var log4js = __importStar(require("log4js"));
var logger = log4js.getLogger();
// logger.level = "debug";
var MemDB = /** @class */ (function (_super) {
    __extends(MemDB, _super);
    // nameConverter: NameConverter;
    function MemDB() {
        return _super.call(this) || this;
    }
    MemDB.prototype.connect = function (arg) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                this.tables = new Map();
                this.nameConverter.init(this);
                return [2 /*return*/];
            });
        });
    };
    MemDB.prototype.disconnect = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/];
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._createTable = function (tableName) {
        return __awaiter(this, void 0, void 0, function () {
            var kvMap;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        if ((_a.sent()) === true) {
                            return [2 /*return*/, tableName];
                        }
                        else {
                            kvMap = new datacell_collections_1.DuplicatedKeyUniqueValueHashMap();
                            this.tables.set(tableName, kvMap);
                            return [2 /*return*/, tableName];
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._hasTable = function (tableName) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, this.tables.has(tableName)];
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.hasTable = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter
                            .makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._hasTable(tableName)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._deleteTable = function (tableName) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        if ((_a.sent()) === true) {
                            this.tables.delete(tableName);
                            return [2 /*return*/, tableName];
                        }
                        else {
                            return [2 /*return*/, null];
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.deleteAllTables = function () {
        return __awaiter(this, void 0, void 0, function () {
            var ks, ks_1, ks_1_1, k, e_1_1;
            var e_1, _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        ks = this.tables.keys();
                        _b.label = 1;
                    case 1:
                        _b.trys.push([1, 6, 7, 8]);
                        ks_1 = __values(ks), ks_1_1 = ks_1.next();
                        _b.label = 2;
                    case 2:
                        if (!!ks_1_1.done) return [3 /*break*/, 5];
                        k = ks_1_1.value;
                        return [4 /*yield*/, this._deleteTable(k)];
                    case 3:
                        _b.sent();
                        _b.label = 4;
                    case 4:
                        ks_1_1 = ks_1.next();
                        return [3 /*break*/, 2];
                    case 5: return [3 /*break*/, 8];
                    case 6:
                        e_1_1 = _b.sent();
                        e_1 = { error: e_1_1 };
                        return [3 /*break*/, 8];
                    case 7:
                        try {
                            if (ks_1_1 && !ks_1_1.done && (_a = ks_1.return)) _a.call(ks_1);
                        }
                        finally { if (e_1) throw e_1.error; }
                        return [7 /*endfinally*/];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.getAllTablesIncludingManagementTables = function () {
        return __awaiter(this, void 0, void 0, function () {
            var iter;
            return __generator(this, function (_a) {
                iter = this.tables.keys();
                return [2 /*return*/, new stream_1.Readable({
                        objectMode: true,
                        read: function () {
                            var k = iter.next();
                            if (!k.done) {
                                this.push(k.value);
                            }
                            else {
                                this.push(null);
                            }
                        }
                    })];
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._getIDs = function (tableName) {
        return __awaiter(this, void 0, void 0, function () {
            var hashmap, ks, iter;
            return __generator(this, function (_a) {
                hashmap = this.tables.get(tableName);
                ks = hashmap.keySet();
                iter = ks.values();
                return [2 /*return*/, new stream_1.Readable({
                        objectMode: true,
                        read: function () {
                            var k = iter.next();
                            if (!k.done) {
                                this.push(k.value);
                            }
                            else {
                                this.push(null);
                            }
                        }
                    })];
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._getPredicates = function (category, objectID) {
        return __awaiter(this, void 0, void 0, function () {
            var r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getAllTables()];
                    case 1:
                        r_stream = _a.sent();
                        return [2 /*return*/, r_stream
                                .pipe(new streamlib.Filter(function (tableName) { return __awaiter(_this, void 0, void 0, function () {
                                var names;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            names = this.nameConverter.parseTableName(tableName);
                                            return [4 /*yield*/, this.nameConverter.getOriginalName(names[0])];
                                        case 1: return [2 /*return*/, (_a.sent()) === category];
                                    }
                                });
                            }); }))
                                .pipe(new streamlib.Filter(function (tableName) {
                                return _this.tables.get(tableName).keySet().has(objectID);
                            }))
                                .pipe(new streamlib.Map(function (tableName) { return __awaiter(_this, void 0, void 0, function () {
                                var names, predicate;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            names = this.nameConverter.parseTableName(tableName);
                                            return [4 /*yield*/, this.nameConverter.getOriginalName(names[1])];
                                        case 1:
                                            predicate = _a.sent();
                                            return [2 /*return*/, predicate];
                                    }
                                });
                            }); }))
                                .pipe(new streamlib.Unique())];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.getPredicates = function (cond) {
        return this._getPredicates(cond.category, cond.objectId);
    };
    /** @inheritdoc */
    MemDB.prototype._getValues = function (tableName, objectID) {
        return __awaiter(this, void 0, void 0, function () {
            var valueSet, iter, _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        valueSet = null;
                        iter = null;
                        return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        _a = (_b.sent());
                        if (!_a) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._hasID(tableName, objectID)];
                    case 2:
                        _a = (_b.sent());
                        _b.label = 3;
                    case 3:
                        // logger.debug("MemDB::_getValues() : tableName = " + tableName);
                        // logger.debug("MemDB::_getValues() : objectID = " + objectID);
                        if (_a) {
                            valueSet = this.tables.get(tableName).get(objectID);
                            iter = valueSet.keys();
                        }
                        return [2 /*return*/, new stream_1.Readable({
                                objectMode: true,
                                read: function () {
                                    if (valueSet === null) {
                                        this.push(null);
                                    }
                                    else {
                                        var v = iter.next();
                                        if (v.done) {
                                            this.push(null);
                                        }
                                        else {
                                            this.push(v.value);
                                        }
                                    }
                                }
                            })];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.getValues = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [2 /*return*/, this._getValues(tableName, cond.objectId)];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._getRows = function (tableName, objectID) {
        return __awaiter(this, void 0, void 0, function () {
            var names, category, predicate, valueSet, iter;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        names = this.nameConverter.parseTableName(tableName);
                        return [4 /*yield*/, this.nameConverter.getOriginalName(names[0])];
                    case 1:
                        category = _a.sent();
                        return [4 /*yield*/, this.nameConverter.getOriginalName(names[1])];
                    case 2:
                        predicate = _a.sent();
                        valueSet = this.tables.get(tableName).get(objectID);
                        iter = valueSet.keys();
                        return [2 /*return*/, new stream_1.Readable({
                                objectMode: true,
                                read: function () {
                                    if (valueSet === null) {
                                        this.push(null);
                                    }
                                    else {
                                        var value = iter.next();
                                        if (value.done) {
                                            this.push(null);
                                        }
                                        else {
                                            this.push(new DataCell_1.DataCell(category, objectID, predicate, value.value));
                                        }
                                    }
                                }
                            })];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.getRows = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [2 /*return*/, this._getRows(tableName, cond.objectId)];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._getAllRows = function (tableName) {
        return __awaiter(this, void 0, void 0, function () {
            var names, category, predicate, r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        names = this.nameConverter.parseTableName(tableName);
                        return [4 /*yield*/, this.nameConverter.getOriginalName(names[0])];
                    case 1:
                        category = _a.sent();
                        return [4 /*yield*/, this.nameConverter.getOriginalName(names[1])];
                    case 2:
                        predicate = _a.sent();
                        r_stream = null;
                        return [4 /*yield*/, this._hasTable(tableName)];
                    case 3:
                        if (!_a.sent()) return [3 /*break*/, 5];
                        return [4 /*yield*/, this._getIDs(tableName)];
                    case 4:
                        r_stream = _a.sent();
                        return [3 /*break*/, 6];
                    case 5:
                        r_stream = stream_1.Readable.from([]);
                        _a.label = 6;
                    case 6: return [2 /*return*/, r_stream
                            // It should be noted that this map function returns Promise objects.
                            .pipe(new streamlib.Map(function (id) { return __awaiter(_this, void 0, void 0, function () {
                            var values, _a, _b, result;
                            return __generator(this, function (_c) {
                                switch (_c.label) {
                                    case 0:
                                        _b = (_a = streamlib).streamToArray;
                                        return [4 /*yield*/, this._getValues(tableName, id)];
                                    case 1: return [4 /*yield*/, _b.apply(_a, [_c.sent()])];
                                    case 2:
                                        values = _c.sent();
                                        result = [id, values];
                                        return [2 /*return*/, result];
                                }
                            });
                        }); }))
                            // Transform Promise<[string, string[]]> to [string, string[]].
                            .pipe(new stream_1.Transform({
                            defaultEncoding: 'utf8',
                            readableObjectMode: true,
                            writableObjectMode: true,
                            transform: function (chunk, encoding, cb) {
                                return __awaiter(this, void 0, void 0, function () {
                                    var _a;
                                    return __generator(this, function (_b) {
                                        switch (_b.label) {
                                            case 0:
                                                _a = this.push;
                                                return [4 /*yield*/, chunk];
                                            case 1:
                                                _a.apply(this, [_b.sent()]);
                                                cb();
                                                return [2 /*return*/];
                                        }
                                    });
                                });
                            },
                            flush: function (cb) {
                                cb();
                            }
                        }))
                            .pipe(new stream_1.Transform({
                            defaultEncoding: 'utf8',
                            readableObjectMode: true,
                            writableObjectMode: true,
                            transform: function (chunk, encoding, cb) {
                                var e_2, _a;
                                try {
                                    for (var _b = __values(chunk[1]), _c = _b.next(); !_c.done; _c = _b.next()) {
                                        var v = _c.value;
                                        this.push(new DataCell_1.DataCell(category, chunk[0], predicate, v));
                                    }
                                }
                                catch (e_2_1) { e_2 = { error: e_2_1 }; }
                                finally {
                                    try {
                                        if (_c && !_c.done && (_a = _b.return)) _a.call(_b);
                                    }
                                    finally { if (e_2) throw e_2.error; }
                                }
                                cb();
                            },
                            flush: function (cb) {
                                cb();
                            }
                        }))];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._hasID = function (tableName, objectID) {
        return __awaiter(this, void 0, void 0, function () {
            var valueSet;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        // logger.debug("MemDB::_hasID() : tableName = " + tableName);
                        // logger.debug("MemDB::_hasID() : objectID = " + objectID);
                        if (!(_a.sent())) {
                            return [2 /*return*/, false];
                        }
                        else {
                            valueSet = this.tables.get(tableName).get(objectID);
                            if (valueSet !== undefined && valueSet.size > 0) {
                                return [2 /*return*/, true];
                            }
                            else {
                                return [2 /*return*/, false];
                            }
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.hasID = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [2 /*return*/, this._hasID(tableName, cond.objectId)];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._hasRow = function (tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function () {
            var _a, valueSet, iter, v;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        _a = !(_b.sent());
                        if (_a) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._hasID(tableName, objectID)];
                    case 2:
                        _a = !(_b.sent());
                        _b.label = 3;
                    case 3:
                        if (_a) {
                            return [2 /*return*/, false];
                        }
                        valueSet = this.tables.get(tableName).get(objectID);
                        iter = valueSet.keys();
                        v = iter.next();
                        while (!v.done) {
                            if (v.value === value) {
                                return [2 /*return*/, true];
                            }
                        }
                        return [2 /*return*/, false];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype.hasRow = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._hasRow(tableName, cond.objectId, cond.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._addRow = function (tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                // This method assumes that the table has already existed.
                if (this.tables.get(tableName).containsKey(objectID)) {
                    this.tables.get(tableName).get(objectID).add(value);
                }
                else {
                    this.tables.get(tableName).put(objectID, value);
                }
                return [2 /*return*/];
            });
        });
    };
    MemDB.prototype._deleteID = function (tableName, objectID) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                this.tables.get(tableName).removeKey(objectID);
                return [2 /*return*/];
            });
        });
    };
    /** @inheritdoc */
    MemDB.prototype._deleteRow = function (tableName, id, value) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                this.tables.get(tableName).get(id).delete(value);
                return [2 /*return*/];
            });
        });
    };
    MemDB.prototype._categoryToObjectIDs = function (category) {
        return __awaiter(this, void 0, void 0, function () {
            var tableObj, r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        tableObj = this.tables;
                        return [4 /*yield*/, this.getAllTables()];
                    case 1:
                        r_stream = _a.sent();
                        return [2 /*return*/, r_stream
                                .pipe(new streamlib.Filter(function (tableName) { return __awaiter(_this, void 0, void 0, function () {
                                var names, c;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            names = this.nameConverter.parseTableName(tableName);
                                            return [4 /*yield*/, this.nameConverter.getOriginalName(names[0])];
                                        case 1:
                                            c = _a.sent();
                                            return [2 /*return*/, category === c];
                                    }
                                });
                            }); }))
                                .pipe(new stream_1.Transform({
                                defaultEncoding: 'utf8',
                                readableObjectMode: true,
                                writableObjectMode: true,
                                transform: function (tableName, encoding, cb) {
                                    var _this = this;
                                    // ImmutableSet of objectIDs.
                                    var ks = tableObj.get(tableName).keySet();
                                    ks.forEach(function (k) {
                                        _this.push(k);
                                    });
                                    cb();
                                },
                                flush: function (cb) {
                                    cb();
                                }
                            }))];
                }
            });
        });
    };
    return MemDB;
}(AbstractDB_1.AbstractDB));
exports.MemDB = MemDB;
