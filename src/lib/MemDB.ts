
import { DuplicatedKeyUniqueValueHashMap } from "datacell-collections";
import { HashMap, JIterator, ImmutableSet, TreeSet } from "typescriptcollectionsframework";
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
    tables: HashMap<string, DuplicatedKeyUniqueValueHashMap<string, string>>;


    // nameConverter: NameConverter;

    constructor() {
        super();
    }


    async connect(arg?: object): Promise<void> {
        this.tables = new HashMap<string, DuplicatedKeyUniqueValueHashMap<string, string>>();
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
            this.tables.put(tableName, kvMap);

            return tableName;
        }
    }




    /** @inheritdoc */
    async _hasTable(tableName: string): Promise<boolean> {
        return this.tables.containsKey(tableName);
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
            this.tables.remove(tableName);
            return tableName;
        }
        else {
            return null;
        }
    }



    /** @inheritdoc */
    async deleteAllTables(): Promise<void> {
        const ks = this.tables.keySet();
        for (const k of ks) {
            await this._deleteTable(k);
        }
    }




    /** @inheritdoc */
    async getAllTablesIncludingManagementTables(): Promise<Readable> {

        const iter: JIterator<string> = this.tables.keySet().iterator();

        return new Readable({
            objectMode: true,
            read() {
                if (iter.hasNext()) {
                    this.push(iter.next());
                }
                else {
                    this.push(null);
                }
            }

        });
    }




    /** @inheritdoc */
    async _getIDs(tableName: string): Promise<Readable> {

        const keys = this.tables.get(tableName).keySet();
        const iter: JIterator<string> = keys.iterator();

        return new Readable({
            objectMode: true,
            read() {
                if (iter.hasNext()) {
                    this.push(iter.next());
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
                return this.tables.get(tableName).keySet().contains(objectID);
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

        let tset: TreeSet<any> = null;
        let iter: JIterator<string> = null;

        // logger.debug("MemDB::_getValues() : tableName = " + tableName);
        // logger.debug("MemDB::_getValues() : objectID = " + objectID);

        if (await this._hasTable(tableName) && await this._hasID(tableName, objectID)) {
            tset = this.tables.get(tableName).get(objectID);
            iter = tset.iterator();
        }

        return new Readable({
            objectMode: true,
            read() {
                if (tset === null) {
                    this.push(null);
                }
                else if (iter.hasNext()) {
                    this.push(iter.next());
                }
                else {
                    this.push(null);
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

        const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);
        let iter: JIterator<any> = null;
        if (tset !== null) {
            iter = tset.iterator();
        }

        return new Readable({
            objectMode: true,
            read() {
                if (tset === null) {
                    this.push(null);
                }
                else if (iter.hasNext()) {
                    const value: string = iter.next();
                    this.push(new DataCell(category, objectID, predicate, value));
                }
                else {
                    this.push(null);
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
            const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);

            if (tset !== undefined && tset.size() > 0) {
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
    async _hasRow(tableName: string, objectID: string, value: any): Promise<boolean> {

        if (!await this._hasTable(tableName) || ! await this._hasID(tableName, objectID)) {
            return false;
        }


        const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);

        for (const iter: JIterator<string>
            = tset.iterator();
            iter.hasNext();) {

            const v: string = iter.next();
            if (this._compareObjectValues(value, v)) {
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




    /** @inheritdoc */
    async _putRowWithReplacingValue(tableName: string, objectID: string, value: any): Promise<void> {
        if (!await this._hasTable(tableName)) {
            await this._createTable(tableName);
        }

        if (! await this._hasRow(tableName, objectID, value)) {
            this.tables.get(tableName).set(objectID, value);
        }
    }



    /** @inheritdoc */
    async deleteRow(cond: DataCell): Promise<void> {
        const tableName: string
            = await this.nameConverter.makeTableName(cond.category, cond.predicate);

        this.tables.get(tableName).get(cond.objectId).remove(cond.value);
    }



    async _categoryToObjectIDs(category: string): Promise<Readable> {

        // tableName => DuplicatedKeyUniqueValueHashMap(id, value)
        const tableObj: HashMap<string, DuplicatedKeyUniqueValueHashMap<string, string>> = this.tables;

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
                    const ks: ImmutableSet<string> = tableObj.get(tableName).keySet();
                    for (const iter: JIterator<string> = ks.iterator(); iter.hasNext();) {
                        this.push(iter.next());
                    }
                    cb();
                },
                flush(cb) {
                    cb();
                }
            }));

    }


}




