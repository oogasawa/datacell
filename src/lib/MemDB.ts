
import { DuplicatedKeyUniqueValueHashMap, StringComparator } from "datacell-collections";
import { HashMap, JIterator, ImmutableSet, TreeSet } from "typescriptcollectionsframework";
import { AbstractDB } from "./AbstractDB";
import { DataCell } from "./DataCell";
import { Readable } from "stream";


import * as log4js from "log4js";
const logger = log4js.getLogger();
logger.level = "debug";



export class MemDB extends AbstractDB {

    // dbName: string;

    // tableName => (category, predicate)
    tables: HashMap<string, DuplicatedKeyUniqueValueHashMap<string, string>>;


    // nameConverter: NameConverter;


    constructor() {
        super();
        this.tables = new HashMap<string, DuplicatedKeyUniqueValueHashMap<string, string>>();
        this.nameConverter.init(this);
    }


    close(): void {
        // nothing to do.
    }



    /** @inheritdoc */
    _createTable(tableName: string): string {

        if (this._hasTable(tableName) === true) {
            return null;
        }

        const kvMap = new DuplicatedKeyUniqueValueHashMap<string, any>();
        this.tables.put(tableName, kvMap);


        return tableName;
    }




    /** @inheritdoc */
    _hasTable(tableName: string): boolean {
        return this.tables.containsKey(tableName);
    }


    /** @inheritdoc */
    hasTable(cond: DataCell): boolean {
        const tableName = this.nameConverter
            .makeTableName(cond.category, cond.predicate);

        return this._hasTable(tableName);
    }


    /** @inheritdoc */
    _deleteTable(tableName: string): string {
        if (this._hasTable(tableName) === true) {
            this.tables.remove(tableName);
            return tableName;
        }
        else {
            return null;
        }
    }



    /** @inheritdoc */
    deleteAllTables(): void {
        const ks = this.tables.keySet();
        for (const k of ks) {
            this._deleteTable(k);
        }
    }




    /** @inheritdoc */
    getAllTablesIncludingManagementTables(): string[] {
        const result: string[] = [];

        // logger.debug("MemDB::getAllTables(): " + this.tables.keySet());

        for (const iter: JIterator<string>
            = this.tables.keySet().iterator();
            iter.hasNext();) {

            const name: string = iter.next();
            result.push(name);
        }

        return result;
    }




    /** @inheritdoc */
    _getIDs(tableName: string): string[] {

        const result: string[] = [];
        const keys = this.tables.get(tableName).keySet();
        for (const iter: JIterator<string> = keys.iterator(); iter.hasNext();) {
            result.push(iter.next()); // This method pushes only "IDs" to the stream.
        }
        result.push(null);

        return result;
    }



    /** @inheritdoc */
    _getIDStream(tableName: string): Readable {

        const readable = new Readable({ objectMode: true });

        // const result: string[] = [];
        const keys = this.tables.get(tableName).keySet();
        for (const iter: JIterator<string> = keys.iterator(); iter.hasNext();) {
            readable.push(iter.next()); // This method pushes only "IDs" to the stream.
        }
        readable.push(null);

        return readable;
    }



    /** @inheritdoc */
    _getPredicates(category: string, objectID: string): string[] {
        const result: string[] = [];
        const tables: string[] = this.getAllTables();
        for (const t of tables) {
            const names = this.nameConverter.parseTableName(t);
            if (this.nameConverter.getOriginalName(names[0]) !== category) {
                continue;
            }
            const predicate = this.nameConverter.getOriginalName(names[1]);

            if (this.tables.get(t).keySet().contains(objectID)) {
                result.push(predicate);
            }
        }
        return result;
    }


    /** @inheritdoc */
    getPredicates(cond: DataCell): string[] {
        return this._getPredicates(cond.category, cond.objectId);
    }



    /** @inheritdoc */
    _getValues(tableName: string, objectID: string): string[] {

        const result: string[] = [];

        if (!this._hasTable(tableName)) {
            return result;
        }

        if (!this._hasID(tableName, objectID)) {
            return result;
        }

        const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);

        for (const iter: JIterator<string>
            = tset.iterator();
            iter.hasNext();) {

            const value: string = iter.next();
            result.push(value);
        }

        return result;
    }


    /** @inheritdoc */
    getValues(cond: DataCell): string[] {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getValues(tableName, cond.objectId);
    }



    /** @inheritdoc */
    _getRows(tableName: string, objectID: string): DataCell[] {

        const result: DataCell[] = [];

        const names: string[] = this.nameConverter.parseTableName(tableName);
        const category: string = this.nameConverter.getOriginalName(names[0]);
        const predicate: string = this.nameConverter.getOriginalName(names[1]);

        const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);
        for (const iter: JIterator<any>
            = tset.iterator();
            iter.hasNext();) {

            const value: string = iter.next();
            result.push(new DataCell(category, objectID, predicate, value));
        }
        return result;
    }



    /** @inheritdoc */
    getRows(cond: DataCell): DataCell[] {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._getRows(tableName, cond.objectId);
    }



    /** @inheritdoc */
    _getAllRows(tableName: string): DataCell[] {

        const result: DataCell[] = [];

        const names: string[] = this.nameConverter.parseTableName(tableName);
        const category: string = this.nameConverter.getOriginalName(names[0]);
        const predicate: string = this.nameConverter.getOriginalName(names[1]);


        const ids: ImmutableSet<string> = this.tables.get(tableName).keySet();
        for (const iter: JIterator<string> = ids.iterator(); iter.hasNext();) {
            const objectID = iter.next();
            const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);
            for (const iter2: JIterator<any> = tset.iterator(); iter2.hasNext();) {

                const value: string = iter2.next();
                result.push(new DataCell(category, objectID, predicate, value));
            }
        }
        return result;
    }



    /** @inheritdoc */
    _getStreamOfAllRows(tableName: string): Readable {

        const readable: Readable = new Readable({ objectMode: true });

        const names: string[] = this.nameConverter.parseTableName(tableName);
        const category: string = this.nameConverter.getOriginalName(names[0]);
        const predicate: string = this.nameConverter.getOriginalName(names[1]);


        const ids: ImmutableSet<string> = this.tables.get(tableName).keySet();
        for (const iter: JIterator<string> = ids.iterator(); iter.hasNext();) {
            const objectID = iter.next();
            const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);
            for (const iter2: JIterator<any> = tset.iterator(); iter2.hasNext();) {

                const value: string = iter2.next();
                readable.push(new DataCell(category, objectID, predicate, value));
            }
        }
        return readable;
    }




    /** @inheritdoc */
    _hasID(tableName: string, objectID: string): boolean {

        if (!this._hasTable(tableName)) {
            return false;
        }

        const tset: TreeSet<any> = this.tables.get(tableName).get(objectID);

        if (tset !== undefined && tset.size() > 0) {
            return true;
        }
        else {
            return false;
        }
    }


    /** @inheritdoc */
    hasID(cond: DataCell): boolean {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasID(tableName, cond.objectId);
    }


    /** @inheritdoc */
    _hasRow(tableName: string, objectID: string, value: any): boolean {

        if (!this._hasTable(tableName) || !this._hasID(tableName, objectID)) {
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
    hasRow(cond: DataCell): boolean {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);
        return this._hasRow(tableName, cond.objectId, cond.value);
    }



    /** @inheritdoc */
    // _compareObjectValues(obj1: any, obj2: any) {
    //     let str1 = JSON.stringify(obj1);
    //     let str2 = JSON.stringify(obj2);

    //     return str1 === str2;
    // }



    /** @inheritdoc */
    _addRow(tableName: string, objectID: string, value: any): void {

        // This method assumes that the table has already existed.

        if (this.tables.get(tableName).containsKey(objectID)) {
            this.tables.get(tableName).get(objectID).add(value);
        }
        else {
            this.tables.get(tableName).put(objectID, value);
        }
    }


    /** @inheritdoc */
    // addRow(cell: DataCell): void {
    //     const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
    //     return this._addRow(tableName, cell.objectId, cell.value);
    // }




    /** @inheritdoc */
    // _putRow(tableName: string, objectID: string, value: any): void {
    //     if (!this._hasTable(tableName)) {
    //         this._createTable(tableName);
    //     }
    //     this._addRow(tableName, objectID, value);
    // }


    /** @inheritdoc */
    putRow(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRow(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    // _putRowIfKeyValuePairIsAbsent(tableName: string, objectID: string, value: any): void {
    //     if (!this._hasTable(tableName)) {
    //         this._createTable(tableName);
    //     }

    //     this._addRow(tableName, objectID, value);
    // }


    /** @inheritdoc */
    putRowIfKeyValuePairIsAbsent(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyValuePairIsAbsent(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    // _putRowIfKeyIsAbsent(tableName: string, objectID: string, value: any): void {
    //     if (!this._hasTable(tableName)) {
    //         this._createTable(tableName);
    //     }

    //     if (!this._hasID(tableName, objectID)) {
    //         this._addRow(tableName, objectID, value);
    //     }
    // }


    /** @inheritdoc */
    putRowIfKeyIsAbsent(cell: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
        return this._putRowIfKeyIsAbsent(tableName, cell.objectId, cell.value);
    }


    /** @inheritdoc */
    _putRowWithReplacingValue(tableName: string, objectID: string, value: any): void {
        if (!this._hasTable(tableName)) {
            this._createTable(tableName);
        }

        if (!this._hasRow(tableName, objectID, value)) {
            this.tables.get(tableName).set(objectID, value);
        }
    }


    /** @inheritdoc */
    // putRowWithReplacingValue(cell: DataCell): void {
    //     const tableName = this.nameConverter.makeTableName(cell.category, cell.predicate);
    //     return this._putRowWithReplacingValue(tableName, cell.objectId, cell.value);
    // }




    /** @inheritdoc */
    deleteRow(cond: DataCell): void {
        const tableName = this.nameConverter.makeTableName(cond.category, cond.predicate);

        this.tables.get(tableName).get(cond.objectId).remove(cond.value);
    }




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

    // /** @inheritdoc */
    // print(): void {

    //     const categories = this.getAllCategories().sort();
    //     // console.log("categories.length: " + categories.length);
    //     for (const c of categories) {
    //         const ids: string[] = this._categoryToObjectIDs(c).sort();
    //         // console.log("  ids.length: " + ids.length);
    //         for (const i of ids) {
    //             const preds: string[] = this._getPredicates(c, i).sort();
    //             // console.log("    preds.length: " + preds.length);
    //             for (const p of preds) {
    //                 const values: any[] = this.getValues(new DataCell(c, i, p, ""));
    //                 // console.log("    values.length: " + values.length);

    //                 for (const v of values) {
    //                     console.log(c + "\t" + i + "\t" + p + "\t" + JSON.stringify(v));
    //                 }
    //             }
    //         }
    //     }

    // }



    _categoryToObjectIDs(category: string): string[] {
        const result: string[] = [];
        const objectIDs = new TreeSet<string>(new StringComparator());

        // the variable "tables" is an array of table names.
        const tables: string[] = this._categoryToTables(category);

        for (const t of tables) { // t is a table name.
            const ks = this.tables.get(t).keySet();
            for (const iter: JIterator<string> = ks.iterator();
                iter.hasNext();) {

                objectIDs.add(iter.next());
            }
        }


        for (const iter: JIterator<string> = objectIDs.iterator();
            iter.hasNext();) {

            result.push(iter.next());
        }

        return result;

    }





}




// -----






