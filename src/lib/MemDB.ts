
import { DuplicatedKeyUniqueValueHashMap } from "datacell-collections";
import { AbstractDB } from "./AbstractDB";
import { DataCell } from "./DataCell";
import { Readable, Transform } from "stream";
import * as streamlib from "datacell-streamlib";

import * as log4js from "log4js";
const logger = log4js.getLogger();
// logger.level = "debug";



export class MemDB extends AbstractDB {

    // dbName: string;

    // tableName => DuplicatedKeyUniqueValueHashMap(id, value)
    tables: Map<string, DuplicatedKeyUniqueValueHashMap<string, string>>;


    // nameConverter: NameConverter;

    constructor() {
        super();
    }


    async connect(arg?: object): Promise<void> {
        this.tables = new Map<string, DuplicatedKeyUniqueValueHashMap<string, string>>();
        this.nameConverter.init(this);
        // nothing to do.
    }

    async disconnect(): Promise<void> {
        // nothing to do.
    }



    /** @inheritdoc */
    async _createTable(tableName: string): Promise<string> {

        if (await this._hasTable(tableName) === true) {
            return tableName;
        }
        else {
            const kvMap = new DuplicatedKeyUniqueValueHashMap<string, any>();
            this.tables.set(tableName, kvMap);

            return tableName;
        }
    }




    /** @inheritdoc */
    async _hasTable(tableName: string): Promise<boolean> {
        return this.tables.has(tableName);
    }


    /** @inheritdoc */
    async hasTable(cond: DataCell): Promise<boolean> {
        const tableName: string
            = await this.nameConverter
                .makeTableName(cond.category, cond.predicate);

        return await this._hasTable(tableName);
    }


    /** @inheritdoc */
    async _deleteTable(tableName: string): Promise<string> {
        if (await this._hasTable(tableName) === true) {
            this.tables.delete(tableName);
            return tableName;
        }
        else {
            return null;
        }
    }



    /** @inheritdoc */
    async deleteAllTables(): Promise<void> {
        const ks: IterableIterator<string> = this.tables.keys();
        for (const k of ks) {
            await this._deleteTable(k);
        }
    }




    /** @inheritdoc */
    async getAllTablesIncludingManagementTables(): Promise<Readable> {

        const iter: IterableIterator<string> = this.tables.keys();

        return new Readable({
            objectMode: true,
            read() {
                const k: IteratorResult<string> = iter.next();
                if (!k.done) {
                    this.push(k.value);
                }
                else {
                    this.push(null);
                }
            }

        });
    }



    /** @inheritdoc */
    async _getIDs(tableName: string): Promise<Readable> {

        const hashmap: DuplicatedKeyUniqueValueHashMap<string, string> = this.tables.get(tableName);
        const ks: Set<string> = hashmap.keySet();

        const iter: IterableIterator<string> = ks.values();

        return new Readable({
            objectMode: true,
            read() {
                const k: IteratorResult<string> = iter.next();
                if (!k.done) {
                    this.push(k.value);
                }
                else {
                    this.push(null);
                }
            }
        });
    }




    /** @inheritdoc */
    async _getPredicates(category: string, objectID: string): Promise<Readable> {

        const r_stream: Readable = await this.getAllTables();

        return r_stream
            .pipe(new streamlib.Filter(async (tableName) => {
                // names = [category, predicate]
                const names = this.nameConverter.parseTableName(tableName);
                return await this.nameConverter.getOriginalName(names[0]) === category;
            }))
            .pipe(new streamlib.Filter((tableName) => {
                return this.tables.get(tableName).keySet().has(objectID);
            }))
            .pipe(new streamlib.Map(async (tableName) => {
                // names = [category, predicate]
                const names: string[] = this.nameConverter.parseTableName(tableName);
                const predicate: string = await this.nameConverter.getOriginalName(names[1]);
                return predicate;
            }))
            .pipe(new streamlib.Unique());

    }


    /** @inheritdoc */
    getPredicates(cond: DataCell): Promise<Readable> {
        return this._getPredicates(cond.category, cond.objectId);
    }



    /** @inheritdoc */
    async _getValues(tableName: string, objectID: string): Promise<Readable> {

        let valueSet: Set<any> = null;
        let iter: IterableIterator<string> = null;

        // logger.debug("MemDB::_getValues() : tableName = " + tableName);
        // logger.debug("MemDB::_getValues() : objectID = " + objectID);

        if (await this._hasTable(tableName) && await this._hasID(tableName, objectID)) {
            valueSet = this.tables.get(tableName).get(objectID);
            iter = valueSet.keys();
        }

        return new Readable({
            objectMode: true,
            read() {
                if (valueSet === null) {
                    this.push(null);
                }
                else {
                    const v: IteratorResult<string> = iter.next();
                    if (v.done) {
                        this.push(null);
                    }
                    else {
                        this.push(v.value);
                    }
                }
            }
        });

    }



    /** @inheritdoc */
    async getValues(cond: DataCell): Promise<Readable> {
        const tableName: string
            = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getValues(tableName, cond.objectId);
    }



    /** @inheritdoc */
    async _getRows(tableName: string, objectID: string): Promise<Readable> { // DataCell[] {

        const names: string[] = this.nameConverter.parseTableName(tableName);
        const category: string = await this.nameConverter.getOriginalName(names[0]);
        const predicate: string = await this.nameConverter.getOriginalName(names[1]);

        const valueSet: Set<string> = this.tables.get(tableName).get(objectID);
        const iter: IterableIterator<string> = valueSet.keys();

        return new Readable({
            objectMode: true,
            read() {
                if (valueSet === null) {
                    this.push(null);
                }
                else {
                    const value: IteratorResult<string> = iter.next();
                    if (value.done) {
                        this.push(null);
                    }
                    else {
                        this.push(new DataCell(category, objectID, predicate, value.value));
                    }
                }
            }
        });

    }



    /** @inheritdoc */
    async getRows(cond: DataCell): Promise<Readable> {
        const tableName: string
            = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getRows(tableName, cond.objectId);
    }



    /** @inheritdoc */
    async _getAllRows(tableName: string): Promise<Readable> { // Returns Readable stream of DataCell objects.

        const names: string[] = this.nameConverter.parseTableName(tableName);
        const category: string = await this.nameConverter.getOriginalName(names[0]);
        const predicate: string = await this.nameConverter.getOriginalName(names[1]);


        let r_stream: Readable = null;
        if (await this._hasTable(tableName)) {
            r_stream = await this._getIDs(tableName);
        }
        else {
            r_stream = Readable.from([]);
        }

        return r_stream
            // It should be noted that this map function returns Promise objects.
            .pipe(new streamlib.Map(async (id) => {
                const values: string[] = await streamlib.streamToArray(await this._getValues(tableName, id));
                const result: [string, string[]] = [id, values];
                return result;
            }))
            // Transform Promise<[string, string[]]> to [string, string[]].
            .pipe(new Transform({
                defaultEncoding: 'utf8',
                readableObjectMode: true,
                writableObjectMode: true,
                async transform(chunk: Promise<[string, string[]]>, encoding: string, cb: Function) {
                    this.push(await chunk);
                    cb();
                },
                flush(cb: Function) {
                    cb();
                }
            }))
            .pipe(new Transform({
                defaultEncoding: 'utf8',
                readableObjectMode: true,
                writableObjectMode: true,
                transform(chunk: [string, string[]], encoding: string, cb: Function) {
                    for (const v of chunk[1]) {
                        this.push(new DataCell(category, chunk[0], predicate, v));
                    }
                    cb();
                },
                flush(cb: Function) {
                    cb();
                }
            }));

    }





    /** @inheritdoc */
    async _hasID(tableName: string, objectID: string): Promise<boolean> {

        // logger.debug("MemDB::_hasID() : tableName = " + tableName);
        // logger.debug("MemDB::_hasID() : objectID = " + objectID);

        if (!await this._hasTable(tableName)) {
            return false;
        }
        else {
            const valueSet: Set<string> = this.tables.get(tableName).get(objectID);

            if (valueSet !== undefined && valueSet.size > 0) {
                return true;
            }
            else {
                return false;
            }
        }
    }


    /** @inheritdoc */
    async hasID(cond: DataCell): Promise<boolean> {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasID(tableName, cond.objectId);
    }


    /** @inheritdoc */
    async _hasRow(tableName: string, objectID: string, value: string): Promise<boolean> {

        if (!await this._hasTable(tableName) || ! await this._hasID(tableName, objectID)) {
            return false;
        }


        const valueSet: Set<string> = this.tables.get(tableName).get(objectID);

        const iter: IterableIterator<string> = valueSet.keys();
        const v: IteratorResult<string> = iter.next();
        while (!v.done) {
            if (v.value === value) {
                return true;
            }
        }

        return false;
    }


    /** @inheritdoc */
    async hasRow(cond: DataCell): Promise<boolean> {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return await this._hasRow(tableName, cond.objectId, cond.value);
    }




    /** @inheritdoc */
    async _addRow(tableName: string, objectID: string, value: any): Promise<void> {

        // This method assumes that the table has already existed.

        if (this.tables.get(tableName).containsKey(objectID)) {
            this.tables.get(tableName).get(objectID).add(value);
        }
        else {
            this.tables.get(tableName).put(objectID, value);
        }
    }



    async _deleteID(tableName: string, objectID: string): Promise<string> {
        this.tables.get(tableName).removeKey(objectID);
        return;
    }



    /** @inheritdoc */
    async deleteRow(cond: DataCell): Promise<void> {
        const tableName: string
            = await this.nameConverter.makeTableName(cond.category, cond.predicate);

        this.tables.get(tableName).get(cond.objectId).delete(cond.value);
    }



    async _categoryToObjectIDs(category: string): Promise<Readable> {

        // tableName => DuplicatedKeyUniqueValueHashMap(id, value)
        const tableObj: Map<string, DuplicatedKeyUniqueValueHashMap<string, string>> = this.tables;

        const r_stream: Readable = await this.getAllTables();

        return r_stream
            .pipe(new streamlib.Filter(async (tableName) => {
                // names = [category, predicate]
                const names: string[] = this.nameConverter.parseTableName(tableName);
                const c: string = await this.nameConverter.getOriginalName(names[0]);
                return category === c;
            }))
            .pipe(new Transform({
                defaultEncoding: 'utf8',
                readableObjectMode: true,
                writableObjectMode: true,
                transform(tableName, encoding, cb) {
                    // ImmutableSet of objectIDs.
                    const ks: Set<string> = tableObj.get(tableName).keySet();
                    ks.forEach((k) => {
                        this.push(k);
                    });
                    cb();
                },
                flush(cb) {
                    cb();
                }
            }));

    }


}
