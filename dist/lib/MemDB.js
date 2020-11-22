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
Object.defineProperty(exports, "__esModule", { value: true });
exports.MemDB = void 0;
const AbstractDB_1 = require("./AbstractDB");
const MemDbTable_1 = require("./MemDbTable");
const DataCell_1 = require("./DataCell");
const stream_1 = require("stream");
const streamlib = __importStar(require("datacell-streamlib"));
const log4js = __importStar(require("log4js"));
const logger = log4js.getLogger();
// logger.level = "debug";
class MemDB extends AbstractDB_1.AbstractDB {
    constructor() {
        super();
    }
    connect(arg) {
        return __awaiter(this, void 0, void 0, function* () {
            this.tables = new Map();
            this.nameConverter.init(this);
        });
    }
    disconnect() {
        return __awaiter(this, void 0, void 0, function* () {
            // nothing to do.
        });
    }
    /** @inheritdoc */
    _createTable(tableName) {
        return __awaiter(this, void 0, void 0, function* () {
            if ((yield this._hasTable(tableName)) === true) {
                return tableName;
            }
            else {
                const table = new MemDbTable_1.MemDbTable();
                this.tables.set(tableName, table);
                return tableName;
            }
        });
    }
    /** @inheritdoc */
    _hasTable(tableName) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.tables.has(tableName);
        });
    }
    /** @inheritdoc */
    _deleteTable(tableName) {
        return __awaiter(this, void 0, void 0, function* () {
            if ((yield this._hasTable(tableName)) === true) {
                this.tables.delete(tableName);
                return tableName;
            }
            else {
                return null;
            }
        });
    }
    /** @inheritdoc */
    getAllTablesIncludingManagementTables() {
        return __awaiter(this, void 0, void 0, function* () {
            const iter = this.tables.keys();
            return new stream_1.Readable({
                objectMode: true,
                read() {
                    const k = iter.next();
                    if (!k.done) {
                        this.push(k.value);
                    }
                    else {
                        this.push(null);
                    }
                }
            });
        });
    }
    /** @inheritdoc */
    _getIDs(tableName) {
        return __awaiter(this, void 0, void 0, function* () {
            const table = this.tables.get(tableName);
            const idIter = table.keys();
            return new stream_1.Readable({
                objectMode: true,
                read() {
                    const idObj = idIter.next();
                    if (idObj.done) {
                        this.push(null);
                    }
                    else {
                        const id = idObj.value;
                        this.push(id);
                    }
                }
            });
        });
    }
    /** @inheritdoc */
    _getPredicates(category, objectID) {
        return __awaiter(this, void 0, void 0, function* () {
            const rst = yield this.getAllTables();
            return rst
                .pipe(streamlib.getAsyncFilter(1, (tableName) => __awaiter(this, void 0, void 0, function* () {
                // names = [category, predicate]
                const names = this.nameConverter.parseTableName(tableName.toString());
                return (yield this.nameConverter.getOriginalName(names[0])) === category;
            })))
                .pipe(streamlib.getAsyncFilter(1, (tableName) => __awaiter(this, void 0, void 0, function* () {
                return this.tables.get(tableName.toString()).has(objectID);
            })))
                .pipe(streamlib.getAsyncMap(1, (tableName) => __awaiter(this, void 0, void 0, function* () {
                // names = [category, predicate]
                const names = this.nameConverter.parseTableName(tableName.toString());
                const predicate = yield this.nameConverter.getOriginalName(names[1]);
                return Buffer.from(predicate, 'utf8');
            })))
                .pipe(new streamlib.Unique());
        });
    }
    /** @inheritdoc */
    _getValues(tableName, objectID) {
        return __awaiter(this, void 0, void 0, function* () {
            const hasTable = yield this._hasTable(tableName);
            const hasId = yield this._hasID(tableName, objectID);
            if (!hasTable || !hasId) {
                return stream_1.Readable.from([]);
            }
            const values = this.tables.get(tableName).get(objectID);
            return stream_1.Readable.from(values);
        });
    }
    /** @inheritdoc */
    _getRows(tableName, objectID) {
        return __awaiter(this, void 0, void 0, function* () {
            // This method returns a Promise of a Readable stream of DataCell objects.
            const r_stream = yield this._getAllRows(tableName);
            const oID = objectID; // renaming the variable for readability.
            return r_stream
                .pipe(new stream_1.Transform({
                readableObjectMode: true,
                writableObjectMode: true,
                transform(chunk, encode, done) {
                    let { category, objectId, predicate, value } = chunk;
                    if (objectId === oID) {
                        this.push(new DataCell_1.DataCell(category, objectId, predicate, value));
                    }
                    done();
                }
            }));
        });
    }
    /** @inheritdoc */
    _getAllRows(tableName) {
        return __awaiter(this, void 0, void 0, function* () {
            const names = this.nameConverter.parseTableName(tableName);
            logger.debug("names: " + names);
            // there is no original name against the maintainance tables,
            // because those tables are not created by calling DataCellStore's put methods.
            const category = yield this.nameConverter.getOriginalName(names[0]);
            const predicate = yield this.nameConverter.getOriginalName(names[1]);
            logger.debug("category: " + category);
            logger.debug("predicate: " + predicate);
            return this.tables.get(tableName)
                .getAllRows()
                .pipe(new stream_1.Transform({
                readableObjectMode: true,
                writableObjectMode: true,
                transform(chunk, encode, done) {
                    let { id, value } = chunk;
                    this.push(new DataCell_1.DataCell(category, id, predicate, value));
                    done();
                }
            }));
        });
    }
    /** @inheritdoc */
    _hasID(tableName, objectID) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(yield this._hasTable(tableName))) {
                return false;
            }
            else {
                return this.tables.get(tableName).has(objectID);
            }
        });
    }
    /** @inheritdoc */
    _hasRow(tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function* () {
            const hasTable = yield this._hasTable(tableName);
            const hasId = yield this._hasID(tableName, objectID);
            if (!hasTable || !hasId) {
                return false;
            }
            const values = this.tables.get(tableName).get(objectID);
            if (values === null) {
                return false;
            }
            else {
                return values.indexOf(value) >= 0;
            }
        });
    }
    /** @inheritdoc */
    _addRow(tableName, objectID, value) {
        return __awaiter(this, void 0, void 0, function* () {
            // This method assumes that the table has already existed.
            if (this.tables.get(tableName).has(objectID)) {
                this.tables.get(tableName).get(objectID).push(value);
            }
            else {
                this.tables.get(tableName).put(objectID, value);
            }
        });
    }
    _deleteID(tableName, objectID) {
        return __awaiter(this, void 0, void 0, function* () {
            this.tables.get(tableName).removeKey(objectID);
            return;
        });
    }
    /** @inheritdoc */
    _deleteRows(tableName, id, value) {
        return __awaiter(this, void 0, void 0, function* () {
            // (id, value) pairs can be exist more than one.
            // This method removes all the (id, value) pairs.
            if (this.tables.get(tableName).has(id)) {
                const values = this.tables.get(tableName).get(id);
                const newValues = [];
                values.forEach((v) => {
                    if (v !== value) {
                        newValues.push(v);
                    }
                });
                this.tables.get(tableName).set(id, newValues);
            }
        });
    }
    _categoryToObjectIDs(category) {
        return __awaiter(this, void 0, void 0, function* () {
            const tableObj = this.tables;
            const r_stream = yield this.getAllTables();
            return r_stream
                .pipe(new streamlib.Filter((tableName) => __awaiter(this, void 0, void 0, function* () {
                // names = [category, predicate]
                const names = this.nameConverter.parseTableName(tableName);
                const c = yield this.nameConverter.getOriginalName(names[0]);
                return category === c;
            })))
                .pipe(new stream_1.Transform({
                defaultEncoding: 'utf8',
                readableObjectMode: true,
                writableObjectMode: true,
                transform(tableName, encoding, done) {
                    // ImmutableSet of objectIDs.
                    const idIter = tableObj.get(tableName).keys();
                    while (true) {
                        const idObj = idIter.next();
                        if (idObj.done) {
                            this.push(null);
                            break;
                        }
                        else {
                            this.push(idObj.value);
                        }
                    }
                    done();
                },
                flush(done) {
                    this.push(null);
                    done();
                }
            }));
        });
    }
}
exports.MemDB = MemDB;
