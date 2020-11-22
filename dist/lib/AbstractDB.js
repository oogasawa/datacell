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
exports.AbstractDB = void 0;
const DataCell_1 = require("./DataCell");
const NameConverter_1 = require("./NameConverter");
const streamlib = __importStar(require("datacell-streamlib"));
const stream_1 = require("stream");
const log4js = __importStar(require("log4js"));
const logger = log4js.getLogger();
// logger.level = 'error';
class AbstractDB {
    constructor() {
        this.nameConverter = new NameConverter_1.NameConverter();
    }
    /** @inheritdoc */
    getNameConverter() {
        return this.nameConverter;
    }
    /** @inheritdoc */
    createTable(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter
                .makeTableName(cond.category, cond.predicate);
            return yield this._createTable(tableName);
        });
    }
    /** @inheritdoc */
    hasTable(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter
                .makeTableName(cond.category, cond.predicate);
            return yield this._hasTable(tableName);
        });
    }
    /** @inheritdoc */
    deleteTable(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter
                .makeTableName(cond.category, cond.predicate);
            return yield this._deleteTable(tableName);
        });
    }
    /** @inheritdoc */
    deleteAllTables() {
        var e_1, _a;
        return __awaiter(this, void 0, void 0, function* () {
            const r_stream = yield this.getAllTables();
            try {
                for (var r_stream_1 = __asyncValues(r_stream), r_stream_1_1; r_stream_1_1 = yield r_stream_1.next(), !r_stream_1_1.done;) {
                    const t = r_stream_1_1.value;
                    yield this._deleteTable(t);
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
    /** @inheritdoc */
    getAllTables() {
        return __awaiter(this, void 0, void 0, function* () {
            const r_stream = yield this.getAllTablesIncludingManagementTables();
            return r_stream
                .pipe(new streamlib.Filter((elem) => {
                return !this._isManagementTable(elem);
            }));
        });
    }
    /** @inheritdoc */
    getAllCategories() {
        return __awaiter(this, void 0, void 0, function* () {
            const r_stream = yield this.getAllTables();
            return r_stream
                // It shoule be noted that this pipe flows Promise<string> objects to the next pipe(),
                // because the map function is asynchronous.
                .pipe(new streamlib.Map((t) => __awaiter(this, void 0, void 0, function* () {
                const names = this.nameConverter.parseTableName(t);
                return yield this.nameConverter.getOriginalName(names[0]);
            })))
                // Transform Promise<string> to string.
                .pipe(new stream_1.Transform({
                readableObjectMode: true,
                writableObjectMode: true,
                transform(chunk, encode, cb) {
                    return __awaiter(this, void 0, void 0, function* () {
                        this.push(yield chunk);
                        cb();
                    });
                },
                flush(cb) {
                    cb();
                }
            }))
                .pipe(new streamlib.Unique());
        });
    }
    /** @inheritdoc */
    getIDs(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cond.category, cond.predicate);
            return this._getIDs(tableName);
        });
    }
    /** @inheritdoc */
    getPredicates(cond) {
        return this._getPredicates(cond.category, cond.objectId);
    }
    /** @inheritdoc */
    getAllPredicates(category) {
        return __awaiter(this, void 0, void 0, function* () {
            const rst = yield this.getAllTables();
            return rst
                .pipe(streamlib.getAsyncFilter(1, (tableName) => __awaiter(this, void 0, void 0, function* () {
                // names = [category, predicate]
                const names = this.nameConverter.parseTableName(tableName.toString());
                const cat = yield this.nameConverter.getOriginalName(names[0]);
                return category === cat;
            })))
                .pipe(streamlib.getAsyncMap(1, (tableName) => __awaiter(this, void 0, void 0, function* () {
                // names = [category, predicate]
                const names = this.nameConverter.parseTableName(tableName.toString());
                const pred = yield this.nameConverter.getOriginalName(names[1]);
                return Buffer.from(pred, 'utf8');
            })));
        });
    }
    /** @inheritdoc */
    getValues(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cond.category, cond.predicate);
            return this._getValues(tableName, cond.objectId);
        });
    }
    /** @inheritdoc */
    getRows(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cond.category, cond.predicate);
            return this._getRows(tableName, cond.objectId);
        });
    }
    /** @inheritdoc */
    getAllRows(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cond.category, cond.predicate);
            return this._getAllRows(tableName);
        });
    }
    /** @inheritdoc */
    hasID(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cond.category, cond.predicate);
            return yield this._hasID(tableName, cond.objectId);
        });
    }
    /** @inheritdoc */
    hasRow(cond) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cond.category, cond.predicate);
            return yield this._hasRow(tableName, cond.objectId, cond.value);
        });
    }
    /** @inheritdoc */
    _compareObjectValues(obj1, obj2) {
        const str1 = JSON.stringify(obj1);
        const str2 = JSON.stringify(obj2);
        return str1 === str2;
    }
    /** @inheritdoc */
    addRow(cell) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cell.category, cell.predicate);
            return yield this._addRow(tableName, cell.objectId, cell.value);
        });
    }
    /** @inheritdoc */
    _putRow(tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(yield this._hasTable(tableName))) {
                yield this._createTable(tableName);
            }
            yield this._addRow(tableName, objectID, value);
        });
    }
    /** @inheritdoc */
    putRow(cell) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cell.category, cell.predicate);
            return yield this._putRow(tableName, cell.objectId, cell.value);
        });
    }
    /** @inheritdoc */
    _putRowIfKeyValuePairIsAbsent(tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(yield this._hasTable(tableName))) {
                yield this._createTable(tableName);
            }
            if (yield this._hasID(tableName, objectID)) {
                const values = yield streamlib.streamToArray(yield this._getValues(tableName, objectID));
                if (values.indexOf(value) < 0) {
                    yield this._addRow(tableName, objectID, value);
                }
            }
            else {
                yield this._addRow(tableName, objectID, value);
            }
        });
    }
    /** @inheritdoc */
    putRowIfKeyValuePairIsAbsent(cell) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cell.category, cell.predicate);
            return yield this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value);
        });
    }
    /** @inheritdoc */
    _putRowIfKeyIsAbsent(tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(yield this._hasTable(tableName))) {
                yield this._createTable(tableName);
            }
            if (!(yield this._hasID(tableName, objectID))) {
                yield this._addRow(tableName, objectID, value);
            }
        });
    }
    /** @inheritdoc */
    putRowIfKeyIsAbsent(cell) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cell.category, cell.predicate);
            return yield this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value);
        });
    }
    /** @inheritdoc */
    _putRowWithReplacingValue(tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(yield this._hasTable(tableName))) {
                yield this._createTable(tableName);
            }
            if (yield this._hasID(tableName, objectID)) {
                yield this._deleteID(tableName, objectID);
            }
            yield this._addRow(tableName, objectID, value);
        });
    }
    /** @inheritdoc */
    putRowWithReplacingValue(cell) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cell.category, cell.predicate);
            return yield this._putRowWithReplacingValue(tableName, cell.objectId, cell.value);
        });
    }
    /** @inheritdoc */
    deleteRows(cell) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableName = yield this.nameConverter.makeTableName(cell.category, cell.predicate);
            return yield this._deleteRows(tableName, cell.objectId, cell.value);
        });
    }
    /** @inheritdoc */
    print() {
        return __awaiter(this, void 0, void 0, function* () {
            const r_stream = yield this.getAllCategories();
            r_stream
                .pipe(new streamlib.Sort())
                .pipe(new streamlib.Map((category) => __awaiter(this, void 0, void 0, function* () {
                const r_stream1 = yield this._categoryToObjectIDs(category);
                r_stream1
                    .pipe(new streamlib.Sort())
                    .pipe(new streamlib.Map((id) => __awaiter(this, void 0, void 0, function* () {
                    const r_stream2 = yield this._getPredicates(category, id);
                    r_stream2
                        .pipe(new streamlib.Sort())
                        .pipe(new streamlib.Map((pred) => __awaiter(this, void 0, void 0, function* () {
                        const r_stream3 = yield this.getValues(new DataCell_1.DataCell(category, id, pred, ""));
                        r_stream3
                            .pipe(new streamlib.Sort())
                            .pipe(new streamlib.Map((value) => {
                            const row = [category,
                                id,
                                pred,
                                JSON.stringify(value)];
                            console.log(row.join("\t"));
                            return value;
                        }));
                        return pred;
                    })));
                    return id;
                })));
                return category;
            })));
        });
    }
    /** @inheritdoc */
    _isManagementTable(tableName) {
        return this.nameConverter.managementTableNames.includes(tableName);
    }
    /** @inheritdoc */
    getManagementTableNames() {
        return this.nameConverter.managementTableNames;
    }
}
exports.AbstractDB = AbstractDB;
// -----
