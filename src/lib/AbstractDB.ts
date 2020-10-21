

import { JIterator, TreeSet } from "typescriptcollectionsframework";
import { DataCellStore } from "./DataCellStore";
import { DataCell } from "./DataCell";
import { NameConverter } from "./NameConverter";
import { StringComparator } from "datacell-collections";
import { Readable } from "stream";

import * as log4js from 'log4js';
const logger = log4js.getLogger();
logger.level = 'error';


export abstract class AbstractDB implements DataCellStore {

    // dbName: string;

    nameConverter: NameConverter;


    constructor() {
        this.nameConverter = new NameConverter();
    }

    /** @inheritdoc */
    abstract close(): void;



    /** @inheritdoc */
    getNameConverter(): NameConverter {
        return this.nameConverter;
    }


    /** @inheritdoc */
    abstract _createTable(tableName: string): string;


    /** @inheritdoc */
    createTable(cond: DataCell): string {
        const tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return this._createTable(tableName);
    }


    /** @inheritdoc */
    abstract _hasTable(tableName: string): boolean;


    /** @inheritdoc */
    hasTable(cond: DataCell): boolean {
        const tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return this._hasTable(tableName);
    }


    /** @inheritdoc */
    abstract _deleteTable(tableName: string): string;




    /** @inheritdoc */
    deleteTable(cond: DataCell): string {
        const tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return this._deleteTable(tableName);
    }



    /** @inheritdoc */
    deleteAllTables(): void {
        const ts = this.getAllTables();
        for (let t of ts) {
            this._deleteTable(t);
        }
    }



    /** @inheritdoc */
    getAllTables(): string[] {
        const result: string[] = [];
        const tables: string[] = this.getAllTablesIncludingManagementTables();

        for (const t of tables) {
            if (!this._isManagementTable(t)) {
                result.push(t);
            }
        }

        return result;
    }


    /** @inheritdoc */
    abstract getAllTablesIncludingManagementTables(): string[];


    /** @inheritdoc */
    getAllCategories(): string[] {
        const result: string[] = [];
        const tset = new TreeSet<string>(new StringComparator());
        const tables: string[] = this.getAllTables();

        // logger.debug("AbstractDB::getAllCategories: " + tables);

        // unique the category names.
        for (const t of tables) {
            const names = this.nameConverter.parseTableName(t);
            if (names.length === 2) {
                tset.add(names[0]);
            }
        }


        for (const iter: JIterator<string> = tset.iterator();
            iter.hasNext();) {

            result.push(this.nameConverter.getOriginalName(iter.next()));
        }

        return result;
    }

    /** @inheritdoc */
    abstract _getIDs(tableName: string): string[];


    /** @inheritdoc */
    getIDs(cond: DataCell): string[] {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getIDs(tableName);
    }



    /** @inheritdoc */
    abstract _getIDStream(tableName: string): Readable;


    /** @inheritdoc */
    getIDStream(cond: DataCell): Readable {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getIDStream(tableName);
    }



    /** @inheritdoc */
    abstract _getPredicates(category: string, objectID: string): string[];


    /** @inheritdoc */
    getPredicates(cond: DataCell): string[] {
        return this._getPredicates(cond.category, cond.objectId);
    }


    /** @inheritdoc */
    getAllPredicates(category: string): string[] {
        const result: string[] = [];
        const tables: string[] = this.getAllTables();

        for (const t of tables) {

            const names: string[] = this.nameConverter.parseTableName(t);

            const cat = this.nameConverter.getOriginalName(names[0]);
            const pred = this.nameConverter.getOriginalName(names[1]);

            if (category === cat) {
                result.push(pred);
            }
        }

        return result;
    }



    /** @inheritdoc */
    abstract _getValues(tableName: string, objectID: string): string[];



    /** @inheritdoc */
    getValues(cond: DataCell): string[] {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getValues(tableName, cond.objectId);
    }



    /** @inheritdoc */
    abstract _getRows(tableName: string, objectID: string): DataCell[];


    /** @inheritdoc */
    getRows(cond: DataCell): DataCell[] {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getRows(tableName, cond.objectId);
    }



    /** @inheritdoc */
    abstract _getAllRows(tableName: string): DataCell[];


    /** @inheritdoc */
    getAllRows(cond: DataCell): DataCell[] {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getAllRows(tableName);
    }



    /** @inheritdoc */
    abstract _getStreamOfAllRows(tableName: string): Readable;


    /** @inheritdoc */
    getStreamOfAllRows(cond: DataCell): Readable {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getStreamOfAllRows(tableName);
    }




    /** @inheritdoc */
    abstract _hasID(tableName: string, objectID: string): boolean;


    /** @inheritdoc */
    hasID(cond: DataCell): boolean {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasID(tableName, cond.objectId);
    }


    /** @inheritdoc */
    abstract _hasRow(tableName: string, objectID: string, value: any): boolean;


    /** @inheritdoc */
    hasRow(cond: DataCell): boolean {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasRow(tableName, cond.objectId, cond.value);
    }



    /** @inheritdoc */
    _compareObjectValues(obj1: any, obj2: any) {
        const str1 = JSON.stringify(obj1);
        const str2 = JSON.stringify(obj2);

        return str1 === str2;
    }



    /** @inheritdoc */
    abstract _addRow(tableName: string, objectID: string, value: any): void;


    /** @inheritdoc */
    addRow(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._addRow(tableName, cell.objectId, cell.value);
    }




    /** @inheritdoc */
    _putRow(tableName: string, objectID: string, value: any): void {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }
        this._addRow(tableName, objectID, value);
    }


    /** @inheritdoc */
    putRow(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRow(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    _putRowIfKeyValuePairIsAbsent(tableName: string, objectID: string, value: any): void {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }

        this._addRow(tableName, objectID, value);
    }


    /** @inheritdoc */
    putRowIfKeyValuePairIsAbsent(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    _putRowIfKeyIsAbsent(tableName: string, objectID: string, value: any): void {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }

        if (!this._hasID(tableName, objectID)) {
            this._addRow(tableName, objectID, value);
        }
    }


    /** @inheritdoc */
    putRowIfKeyIsAbsent(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    abstract _putRowWithReplacingValue(tableName: string, objectID: string, value: any): void;


    /** @inheritdoc */
    putRowWithReplacingValue(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowWithReplacingValue(tableName, cell.objectId, cell.value);
    }




    /** @inheritdoc */
    abstract deleteRow(cond: DataCell): void;



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
    print(): void {

        const categories = this.getAllCategories().sort();
        // console.log("categories.length: " + categories.length);
        for (const c of categories) {
            const ids: string[] = this._categoryToObjectIDs(c).sort();
            // console.log("  ids.length: " + ids.length);
            for (const i of ids) {
                const preds: string[] = this._getPredicates(c, i).sort();
                // console.log("    preds.length: " + preds.length);
                for (const p of preds) {
                    const values: any[] = this.getValues(new DataCell(c, i, p, ""));
                    // console.log("    values.length: " + values.length);

                    for (const v of values) {
                        console.log(c + "\t" + i + "\t" + p + "\t" + JSON.stringify(v));
                    }
                }
            }
        }

    }


    _categoryToTables(category: string): string[] {
        const result: string[] = [];
        const categoryInternalName = this.nameConverter._makeInternalName(category);
        const tables = this.getAllTables();
        for (const t of tables) {
            const cps = this.nameConverter.parseTableName(t);
            if (cps[0] === categoryInternalName) {
                result.push(t);
            }
        }
        return result;
    }



    _tablesToPredicates(tableList: string[]): string[] {
        const result: string[] = [];
        for (const t of tableList) {
            const cps = this.nameConverter.parseTableName(t);
            result.push(this.nameConverter.getOriginalName(cps[1]));
        }
        return result;
    }


    abstract _categoryToObjectIDs(category: string): string[];



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






