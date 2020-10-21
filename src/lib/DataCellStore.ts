
// import { Stream } from "ts-stream";
import { DataCell } from "./DataCell";
import { NameConverter } from "./NameConverter";
import { Readable } from "stream";

export interface DataCellStore {


    getNameConverter(): NameConverter;


    _createTable(tableName: string): string;


    createTable(cond: DataCell): string;


    _hasTable(tableName: string): boolean;


    hasTable(cond: DataCell): boolean;



    _deleteTable(tableName: string): string;


    deleteTable(cond: DataCell): string;



    deleteAllTables(): void;


	/** Returns the names of all tables.
	 * (The management tables are excluded.)
	 */
    getAllTables(): string[];


    getAllTablesIncludingManagementTables(): string[];

	/** Returns the names of all categories.
	 * (The management tables are excluded.)
	 */
    getAllCategories(): string[];


	/** Returns a list of all objectIDs in a given table.
	 *
	 * @param tableName
	 */
    _getIDs(tableName: string): string[];


	/** Returns a list of all objectIDs in a given category / predicate pair.
	 *
	 * <code>cond.category</code> and <code>cond.predicate</code> will be used for the search condition.
	 * other properties (that is, <code>cond.objectID</code> and <code>cond.value</code> are ignored.
	 *
	 * @param cond - A data cell which represents a search condition.
	 */
    getIDs(cond: DataCell): string[];


	/** Returns a Readable stream of all objectIDs in a given table.
	 *
	 * @param tableName
	 */
    _getIDStream(tableName: string): Readable;


	/** Returns a Readable stream of all objectIDs in a given category / predicate pair.
	 *
	 * <code>cond.category</code> and <code>cond.predicate</code> will be used for the search condition.
	 * other properties (that is, <code>cond.objectID</code> and <code>cond.value</code> are ignored.
	 *
	 * @param cond - A data cell which represents a search condition.
	 */
    getIDStream(cond: DataCell): Readable;



	/** Returns a list of predicates corresponding to a given objectID.
	 *
	 * objectID is assumed that it is unique in a given category. 
	 *
	 * @param category
	 * @param objectID
	 */
    _getPredicates(category: string, objectID: string): string[];


	/** Returns a list of predicates corresponding to a given objectID.
	 *
	 * <code>cond.objectID</code> will be used for the search condition.
	 * other properties are ignored.
	 * @param objectID
	 */
    getPredicates(cond: DataCell): string[];


	/** Returns all predicates exists in a category.
	 *
	 * @param category
	 */
    getAllPredicates(category: string): string[];



	/** Gets values specified by a tableName and an objectID.
	 * 
	 * 
	 *
	 * @param tableName
	 * @param objectID
	 * @return An array of values.
	 * If the given table does not exist, or the given objectID is not in the table, 
	 * this method returns an empty array, <code>[]</code>
	 */
    _getValues(tableName: string, objectID: string): string[];


	/** Gets values specified by category, objectID, and predicate.
	 *
	 * @param cond
	 */
    getValues(cond: DataCell): string[];


	/** Gets a list of data cells specified by a tableName and objectID.
	 *
	 * @param tableName
	 * @param objectID
	 */
    _getRows(tableName: string, objectID: string): DataCell[];


	/** Gets a list of data cells specified by category, objectID and predicate.
	 *
	 * @param cond
	 */
    getRows(cond: DataCell): DataCell[];


	/** Returns an array of all data cells which is stored in a given table.
	 *
	 * @param tableName
	 */
    _getAllRows(tableName: string): DataCell[];



	/** Returns an array of all data cells which is stored in a given table (a category / predicate pair).
	 *
	 * @param tableName
	 * @param objectID
	 */
    getAllRows(cond: DataCell): DataCell[];



	/** Returns a Readable stream of DataCell objects in a given table.
	 *
	 * @param tableName
	 */
    _getStreamOfAllRows(tableName: string, objectID: string): Readable;



	/** Returns a Readable stream of DataCell objects in a given table.
	 * 
	 *
	 * @param cond Condition that specifies a table. category and predicate in the given DataCell is used. 
	 */
    getStreamOfAllRows(cond: DataCell): Readable;




	/** Tests whether a given objectID is contained in a given table.
	 *
	 * @param tableName
	 * @param objectID
	 */
    _hasID(tableName: string, objectID: string): boolean;



	/** Tests whether a given objectID is contained in a given table (a category / predicate pair).
	 *
	 * @param cond
	 */
    hasID(cond: DataCell): boolean;


	/** Tests whether a given objectID / value pair exists or not in a given table.
	 *
	 * @param tableName
	 * @param objectID
	 * @param value
	 */
    _hasRow(tableName: string, objectID: string, value: any): boolean;


	/** Tests whether a given objectID / value pair exists or not in a given table (category / predicate pair).
	 *
	 * @param cond
	 */
    hasRow(cond: DataCell): boolean;


	/** Add a datacell in the data store.
	 *
	 * Existence of the given table will not be checked.
	 *
	 * @param tableName
	 * @param objectID
	 * @param value
	 */
    _addRow(tableName: string, objectID: string, value: any): void;


	/** Add a datacell in the data store.
	 *
	 * Existence of the given table will not be checked.
	 *
	 * @param cell
	 */
    addRow(cell: DataCell): void;


	/** Add a data cell in the data store.
	 *
	 * If the table does not exist, the table will be created.
	 *
	 * @param tableName
	 * @param objectID
	 * @param value
	 */
    _putRow(tableName: string, objectID: string, value: any): void;


	/** Add a data cell in the data store.
	 *
	 * If the table does not exist, the table will be created.
	 *
	 * @param cell
	 */
    putRow(cell: DataCell): void;


    _putRowIfKeyValuePairIsAbsent(tableName: string, objectID: string, value: any): void;


    putRowIfKeyValuePairIsAbsent(cell: DataCell): void;


    _putRowIfKeyIsAbsent(tableName: string, objectID: string, value: any): void;


    putRowIfKeyIsAbsent(cell: DataCell): void;


    _putRowWithReplacingValue(tableName: string, objectID: string, value: any): void;


    putRowWithReplacingValue(cell: DataCell): void;


    deleteRow(cond: DataCell): void;


    _isManagementTable(tableName: string): boolean;


    getManagementTableNames(): string[];


    print(): void;


}

