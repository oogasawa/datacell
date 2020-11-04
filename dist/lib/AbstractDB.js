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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AbstractDB = void 0;
var DataCell_1 = require("./DataCell");
var NameConverter_1 = require("./NameConverter");
var streamlib = __importStar(require("datacell-streamlib"));
var stream_1 = require("stream");
var log4js = __importStar(require("log4js"));
var logger = log4js.getLogger();
// logger.level = 'error';
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
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter
                            .makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._createTable(tableName)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.hasTable = function (cond) {
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
    AbstractDB.prototype.deleteTable = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter
                            .makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._deleteTable(tableName)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.deleteAllTables = function () {
        var e_1, _a;
        return __awaiter(this, void 0, void 0, function () {
            var r_stream, r_stream_1, r_stream_1_1, t, e_1_1;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, this.getAllTables()];
                    case 1:
                        r_stream = _b.sent();
                        _b.label = 2;
                    case 2:
                        _b.trys.push([2, 8, 9, 14]);
                        r_stream_1 = __asyncValues(r_stream);
                        _b.label = 3;
                    case 3: return [4 /*yield*/, r_stream_1.next()];
                    case 4:
                        if (!(r_stream_1_1 = _b.sent(), !r_stream_1_1.done)) return [3 /*break*/, 7];
                        t = r_stream_1_1.value;
                        return [4 /*yield*/, this._deleteTable(t)];
                    case 5:
                        _b.sent();
                        _b.label = 6;
                    case 6: return [3 /*break*/, 3];
                    case 7: return [3 /*break*/, 14];
                    case 8:
                        e_1_1 = _b.sent();
                        e_1 = { error: e_1_1 };
                        return [3 /*break*/, 14];
                    case 9:
                        _b.trys.push([9, , 12, 13]);
                        if (!(r_stream_1_1 && !r_stream_1_1.done && (_a = r_stream_1.return))) return [3 /*break*/, 11];
                        return [4 /*yield*/, _a.call(r_stream_1)];
                    case 10:
                        _b.sent();
                        _b.label = 11;
                    case 11: return [3 /*break*/, 13];
                    case 12:
                        if (e_1) throw e_1.error;
                        return [7 /*endfinally*/];
                    case 13: return [7 /*endfinally*/];
                    case 14: return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllTables = function () {
        return __awaiter(this, void 0, void 0, function () {
            var r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getAllTablesIncludingManagementTables()];
                    case 1:
                        r_stream = _a.sent();
                        return [2 /*return*/, r_stream
                                .pipe(new streamlib.Filter(function (elem) {
                                return !_this._isManagementTable(elem);
                            }))];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllCategories = function () {
        return __awaiter(this, void 0, void 0, function () {
            var r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getAllTables()];
                    case 1:
                        r_stream = _a.sent();
                        return [2 /*return*/, r_stream
                                // It shoule be noted that this pipe flows Promise<string> objects to the next pipe(),
                                // because the map function is asynchronous.
                                .pipe(new streamlib.Map(function (t) { return __awaiter(_this, void 0, void 0, function () {
                                var names;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            names = this.nameConverter.parseTableName(t);
                                            return [4 /*yield*/, this.nameConverter.getOriginalName(names[0])];
                                        case 1: return [2 /*return*/, _a.sent()];
                                    }
                                });
                            }); }))
                                // Transform Promise<string> to string.
                                .pipe(new stream_1.Transform({
                                readableObjectMode: true,
                                writableObjectMode: true,
                                transform: function (chunk, encode, cb) {
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
                                .pipe(new streamlib.Unique())];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.getIDs = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [2 /*return*/, this._getIDs(tableName)];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.getPredicates = function (cond) {
        return this._getPredicates(cond.category, cond.objectId);
    };
    /** @inheritdoc */
    AbstractDB.prototype.getAllPredicates = function (category) {
        return __awaiter(this, void 0, void 0, function () {
            var r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getAllTables()];
                    case 1:
                        r_stream = _a.sent();
                        return [2 /*return*/, r_stream
                                .pipe(new streamlib.Filter(function (t) { return __awaiter(_this, void 0, void 0, function () {
                                var names, cat;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            names = this.nameConverter.parseTableName(t);
                                            return [4 /*yield*/, this.nameConverter.getOriginalName(names[0])];
                                        case 1:
                                            cat = _a.sent();
                                            return [2 /*return*/, category === cat];
                                    }
                                });
                            }); }))
                                .pipe(new streamlib.Map(function (t) { return __awaiter(_this, void 0, void 0, function () {
                                var names, pred;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            names = this.nameConverter.parseTableName(t);
                                            return [4 /*yield*/, this.nameConverter.getOriginalName(names[1])];
                                        case 1:
                                            pred = _a.sent();
                                            return [2 /*return*/, pred];
                                    }
                                });
                            }); }))];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.getValues = function (cond) {
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
    AbstractDB.prototype.getRows = function (cond) {
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
    AbstractDB.prototype.getAllRows = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [2 /*return*/, this._getAllRows(tableName)];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.hasID = function (cond) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cond.category, cond.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._hasID(tableName, cond.objectId)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.hasRow = function (cond) {
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
    AbstractDB.prototype._compareObjectValues = function (obj1, obj2) {
        var str1 = JSON.stringify(obj1);
        var str2 = JSON.stringify(obj2);
        return str1 === str2;
    };
    /** @inheritdoc */
    AbstractDB.prototype.addRow = function (cell) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cell.category, cell.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._addRow(tableName, cell.objectId, cell.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRow = function (tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        if (!!(_a.sent())) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._createTable(tableName)];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3: return [4 /*yield*/, this._addRow(tableName, objectID, value)];
                    case 4:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRow = function (cell) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cell.category, cell.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._putRow(tableName, cell.objectId, cell.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRowIfKeyValuePairIsAbsent = function (tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        if (!!(_a.sent())) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._createTable(tableName)];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3: return [4 /*yield*/, this._addRow(tableName, objectID, value)];
                    case 4:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRowIfKeyValuePairIsAbsent = function (cell) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cell.category, cell.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRowIfKeyIsAbsent = function (tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        if (!!(_a.sent())) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._createTable(tableName)];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3: return [4 /*yield*/, this._hasID(tableName, objectID)];
                    case 4:
                        if (!!(_a.sent())) return [3 /*break*/, 6];
                        return [4 /*yield*/, this._addRow(tableName, objectID, value)];
                    case 5:
                        _a.sent();
                        _a.label = 6;
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRowIfKeyIsAbsent = function (cell) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cell.category, cell.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype._putRowWithReplacingValue = function (tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._hasTable(tableName)];
                    case 1:
                        if (!!(_a.sent())) return [3 /*break*/, 3];
                        return [4 /*yield*/, this._createTable(tableName)];
                    case 2:
                        _a.sent();
                        _a.label = 3;
                    case 3: return [4 /*yield*/, this._hasID(tableName, objectID)];
                    case 4:
                        if (!_a.sent()) return [3 /*break*/, 6];
                        return [4 /*yield*/, this._deleteID(tableName, objectID)];
                    case 5:
                        _a.sent();
                        _a.label = 6;
                    case 6: return [4 /*yield*/, this._addRow(tableName, objectID, value)];
                    case 7:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.putRowWithReplacingValue = function (cell) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cell.category, cell.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._putRowWithReplacingValue(tableName, cell.objectId, cell.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.deleteRow = function (cell) {
        return __awaiter(this, void 0, void 0, function () {
            var tableName;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.nameConverter.makeTableName(cell.category, cell.predicate)];
                    case 1:
                        tableName = _a.sent();
                        return [4 /*yield*/, this._deleteRow(tableName, cell.objectId, cell.value)];
                    case 2: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    /** @inheritdoc */
    AbstractDB.prototype.print = function () {
        return __awaiter(this, void 0, void 0, function () {
            var r_stream;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getAllCategories()];
                    case 1:
                        r_stream = _a.sent();
                        r_stream
                            .pipe(new streamlib.Sort())
                            .pipe(new streamlib.Map(function (category) { return __awaiter(_this, void 0, void 0, function () {
                            var r_stream1;
                            var _this = this;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0: return [4 /*yield*/, this._categoryToObjectIDs(category)];
                                    case 1:
                                        r_stream1 = _a.sent();
                                        r_stream1
                                            .pipe(new streamlib.Sort())
                                            .pipe(new streamlib.Map(function (id) { return __awaiter(_this, void 0, void 0, function () {
                                            var r_stream2;
                                            var _this = this;
                                            return __generator(this, function (_a) {
                                                switch (_a.label) {
                                                    case 0: return [4 /*yield*/, this._getPredicates(category, id)];
                                                    case 1:
                                                        r_stream2 = _a.sent();
                                                        r_stream2
                                                            .pipe(new streamlib.Sort())
                                                            .pipe(new streamlib.Map(function (pred) { return __awaiter(_this, void 0, void 0, function () {
                                                            var r_stream3;
                                                            return __generator(this, function (_a) {
                                                                switch (_a.label) {
                                                                    case 0: return [4 /*yield*/, this.getValues(new DataCell_1.DataCell(category, id, pred, ""))];
                                                                    case 1:
                                                                        r_stream3 = _a.sent();
                                                                        r_stream3
                                                                            .pipe(new streamlib.Sort())
                                                                            .pipe(new streamlib.Map(function (value) {
                                                                            var row = [category,
                                                                                id,
                                                                                pred,
                                                                                JSON.stringify(value)];
                                                                            console.log(row.join("\t"));
                                                                            return value;
                                                                        }));
                                                                        return [2 /*return*/, pred];
                                                                }
                                                            });
                                                        }); }));
                                                        return [2 /*return*/, id];
                                                }
                                            });
                                        }); }));
                                        return [2 /*return*/, category];
                                }
                            });
                        }); }));
                        return [2 /*return*/];
                }
            });
        });
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
