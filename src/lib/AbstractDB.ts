
import { DataCellStore } from "./DataCellStore";
import { DataCell } from "./DataCell";
import { NameConverter } from "./NameConverter";
import * as streamlib from "datacell-streamlib";
import { Readable, Transform } from "stream";


import * as log4js from 'log4js';
const logger = log4js.getLogger();
// logger.level = 'error';


export abstract class AbstractDB implements DataCellStore {

    // dbName: string;

    nameConverter: NameConverter;


    constructor() {
        this.nameConverter = new NameConverter();
    }


    abstract connect(arg?: object): Promise<void>;

    /** @inheritdoc */
    abstract disconnect(): Promise<void>;



    /** @inheritdoc */
    getNameConverter(): NameConverter {
        return this.nameConverter;
    }


    /** @inheritdoc */
    abstract _createTable(tableName: string): Promise<string>;


    /** @inheritdoc */
    async createTable(cond: DataCell): Promise<string> {
        const tableName: string = await this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return await this._createTable(tableName);
    }


    /** @inheritdoc */
    abstract _hasTable(tableName: string): Promise<boolean>;


    /** @inheritdoc */
    async hasTable(cond: DataCell): Promise<boolean> {
        const tableName: string = await this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return await this._hasTable(tableName);
    }


    /** @inheritdoc */
    abstract _deleteTable(tableName: string): Promise<string>;


    /** @inheritdoc */
    async deleteTable(cond: DataCell): Promise<string> {
        const tableName: string = await this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return await this._deleteTable(tableName);
    }



    /** @inheritdoc */
    async deleteAllTables(): Promise<void> {
        const r_stream: Readable = await this.getAllTables();
        for await (const t of r_stream) {
            await this._deleteTable(t);
        }
    }



    /** @inheritdoc */
    async getAllTables(): Promise<Readable> {
        const r_stream: Readable = await this.getAllTablesIncludingManagementTables();
        return r_stream
            .pipe(new streamlib.Filter((elem: string) => {
                return !this._isManagementTable(elem);
            }));
    }



    /** @inheritdoc */
    abstract getAllTablesIncludingManagementTables(): Promise<Readable>;


    /** @inheritdoc */
    async getAllCategories(): Promise<Readable> {

        const r_stream: Readable = await this.getAllTables();

        return r_stream
            // It shoule be noted that this pipe flows Promise<string> objects to the next pipe(),
            // because the map function is asynchronous.
            .pipe(new streamlib.Map(async (t: string) => {
                const names = this.nameConverter.parseTableName(t);
                return await this.nameConverter.getOriginalName(names[0]);
            }))
            // Transform Promise<string> to string.
            .pipe(new Transform({
                readableObjectMode: true,
                writableObjectMode: true,
                async transform(chunk: Promise<string>, encode: string, cb: Function) {
                    this.push(await chunk);
                    cb();
                },
                flush(cb: Function) {
                    cb();
                }
            }))
            .pipe(new streamlib.Unique());

    }

    /** @inheritdoc */
    abstract _getIDs(tableName: string): Promise<Readable>;


    /** @inheritdoc */
    async getIDs(cond: DataCell): Promise<Readable> {

        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getIDs(tableName);
    }



    /** @inheritdoc */
    abstract _getPredicates(category: string, objectID: string): Promise<Readable>;


    /** @inheritdoc */
    getPredicates(cond: DataCell): Promise<Readable> {
        return this._getPredicates(cond.category, cond.objectId);
    }


    /** @inheritdoc */
    async getAllPredicates(category: string): Promise<Readable> {

        const r_stream: Readable = await this.getAllTables();

        return r_stream
            .pipe(new streamlib.Filter(async (t) => { // choose tables which match to the given category.
                // names = [category, predicate]
                const names: string[] = this.nameConverter.parseTableName(t);
                const cat: string = await this.nameConverter.getOriginalName(names[0]);
                return category === cat;
            }))
            .pipe(new streamlib.Map(async (t) => { // map table names to predicates.
                // names = [category, predicate]
                const names: string[] = this.nameConverter.parseTableName(t);
                const pred: string = await this.nameConverter.getOriginalName(names[1]);
                return pred;
            }));

    }



    /** @inheritdoc */
    abstract _getValues(tableName: string, objectID: string): Promise<Readable>;



    /** @inheritdoc */
    async getValues(cond: DataCell): Promise<Readable> {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getValues(tableName, cond.objectId);
    }


    /** @inheritdoc */
    abstract _getRows(tableName: string, objectID: string): Promise<Readable>; // DataCell[];


    /** @inheritdoc */
    async getRows(cond: DataCell): Promise<Readable> { // DataCell[] {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getRows(tableName, cond.objectId);
    }



    /** @inheritdoc */
    abstract _getAllRows(tableName: string): Promise<Readable>; // DataCell[];


    /** @inheritdoc */
    async getAllRows(cond: DataCell): Promise<Readable> { // DataCell[] {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getAllRows(tableName);
    }




    /** @inheritdoc */
    abstract _hasID(tableName: string, objectID: string): Promise<boolean>;


    /** @inheritdoc */
    async hasID(cond: DataCell): Promise<boolean> {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return await this._hasID(tableName, cond.objectId);
    }


    /** @inheritdoc */
    abstract _hasRow(tableName: string, objectID: string, value: any): Promise<boolean>;


    /** @inheritdoc */
    async hasRow(cond: DataCell): Promise<boolean> {
        const tableName: string = await this.nameConverter.makeTableName(cond.category, cond.predicate);
        return await this._hasRow(tableName, cond.objectId, cond.value);
    }



    /** @inheritdoc */
    _compareObjectValues(obj1: any, obj2: any) {
        const str1 = JSON.stringify(obj1);
        const str2 = JSON.stringify(obj2);

        return str1 === str2;
    }



    /** @inheritdoc */
    abstract _addRow(tableName: string, objectID: string, value: any): Promise<void>;



    /** @inheritdoc */
    async addRow(cell: DataCell): Promise<void> {
        const tableName: string = await this.nameConverter.makeTableName(cell.category, cell.predicate);
        return await this._addRow(tableName, cell.objectId, cell.value);
    }



    /** @inheritdoc */
    async _putRow(tableName: string, objectID: string, value: any): Promise<void> {
        if (! await this._hasTable(tableName)) {
            await this._createTable(tableName);
        }
        await this._addRow(tableName, objectID, value);
    }


    /** @inheritdoc */
    async putRow(cell: DataCell): Promise<void> {
        const tableName: string = await this.nameConverter.makeTableName(cell.category, cell.predicate);
        return await this._putRow(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    async _putRowIfKeyValuePairIsAbsent(tableName: string, objectID: string, value: any): Promise<void> {
        if (! await this._hasTable(tableName)) {
            await this._createTable(tableName);
        }

        if (await this._hasID(tableName, objectID)) {
            const values: string[] = await streamlib.streamToArray(
                await this._getValues(tableName, objectID));

            if (values.indexOf(value) < 0) {
                await this._addRow(tableName, objectID, value);
            }
        }
        else {
            await this._addRow(tableName, objectID, value);
        }
    }


    /** @inheritdoc */
    async putRowIfKeyValuePairIsAbsent(cell: DataCell): Promise<void> {
        const tableName: string = await this.nameConverter.makeTableName(cell.category, cell.predicate);
        return await this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    async _putRowIfKeyIsAbsent(tableName: string, objectID: string, value: any): Promise<void> {
        if (!await this._hasTable(tableName)) {
            await this._createTable(tableName);
        }

        if (! await this._hasID(tableName, objectID)) {
            await this._addRow(tableName, objectID, value);
        }
    }


    /** @inheritdoc */
    async putRowIfKeyIsAbsent(cell: DataCell): Promise<void> {
        const tableName: string = await this.nameConverter.makeTableName(cell.category, cell.predicate);
        return await this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value);
    }



    abstract _deleteID(tableName: string, objectID: string): Promise<string>;


    /** @inheritdoc */
    async _putRowWithReplacingValue(tableName: string, objectID: string, value: any): Promise<void> {

        if (! await this._hasTable(tableName)) {
            await this._createTable(tableName);
        }

        if (await this._hasID(tableName, objectID)) {
            await this._deleteID(tableName, objectID);
        }
        await this._addRow(tableName, objectID, value);

    }



    /** @inheritdoc */
    async putRowWithReplacingValue(cell: DataCell): Promise<void> {
        const tableName: string = await this.nameConverter.makeTableName(cell.category, cell.predicate);
        return await this._putRowWithReplacingValue(tableName, cell.objectId, cell.value);
    }




    /** @inheritdoc */
    abstract _deleteRows(tableName: string, id: string, value: string): Promise<void>;



    /** @inheritdoc */
    async deleteRows(cell: DataCell): Promise<void> {
        const tableName: string = await this.nameConverter.makeTableName(cell.category, cell.predicate);
        return await this._deleteRows(tableName, cell.objectId, cell.value);
    }




    /** @inheritdoc */
    async print(): Promise<void> {

        const r_stream: Readable = await this.getAllCategories();

        r_stream
            .pipe(new streamlib.Sort())
            .pipe(new streamlib.Map(async (category) => {
                const r_stream1: Readable = await this._categoryToObjectIDs(category);
                r_stream1
                    .pipe(new streamlib.Sort())
                    .pipe(new streamlib.Map(async (id) => {
                        const r_stream2: Readable = await this._getPredicates(category, id);
                        r_stream2
                            .pipe(new streamlib.Sort())
                            .pipe(new streamlib.Map(async (pred) => {
                                const r_stream3: Readable = await this.getValues(new DataCell(category, id, pred, ""));
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
                            }));
                        return id;
                    }));
                return category;
            }));

    }




    abstract _categoryToObjectIDs(category: string): Promise<Readable>;



    /** @inheritdoc */
    _isManagementTable(tableName: string): boolean {
        return this.nameConverter.managementTableNames.includes(tableName);
    }

    /** @inheritdoc */
    getManagementTableNames(): string[] {
        return this.nameConverter.managementTableNames;
    }


}




// -----






