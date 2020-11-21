
// import { Stream } from "ts-stream";
import { DataCell } from "./DataCell";
import { NameConverter } from "./NameConverter";
import { Readable } from "stream";

export interface DataCellStore {



    connect(arg?: object): Promise<void>;


	/** Close connection to the DataCellStore.
	 *
	 */
    disconnect(): Promise<void>;


    getNameConverter(): NameConverter;


    _createTable(tableName: string): Promise<string>;


    createTable(cond: DataCell): Promise<string>;


    _hasTable(tableName: string): Promise<boolean>;


    hasTable(cond: DataCell): Promise<boolean>;



    _deleteTable(tableName: string): Promise<string>;


    deleteTable(cond: DataCell): Promise<string>;



    deleteAllTables(): Promise<void>;


	/** Returns the names of all tables.
	 * (The management tables are excluded.)
	 */
    getAllTables(): Promise<Readable>;


    getAllTablesIncludingManagementTables(): Promise<Readable>;

	/** Returns the names of all categories.
	 * (The management tables are excluded.)
	 */
    getAllCategories(): Promise<Readable>;


	/** Returns a list of all objectIDs in a given table.
	 *
	 * @param tableName
	 */
    _getIDs(tableName: string): Promise<Readable>;


	/** Returns a list of all objectIDs in a given category / predicate pair.
	 *
	 * <code>cond.category</code> and <code>cond.predicate</code> will be used for the search condition.
	 * other properties (that is, <code>cond.objectID</code> and <code>cond.value</code> are ignored.
	 *
	 * @param cond - A data cell which represents a search condition.
	 */
    getIDs(cond: DataCell): Promise<Readable>;




	/** Returns a list of predicates corresponding to a given objectID.
	 *
	 * objectID is assumed that it is unique in a given category. 
	 *
	 * @param category
	 * @param objectID
	 */
    _getPredicates(category: string, objectID: string): Promise<Readable>;


	/** Returns a list of predicates corresponding to a given objectID.
	 *
	 * <code>cond.objectID</code> will be used for the search condition.
	 * other properties are ignored.
	 * @param objectID
	 */
    getPredicates(cond: DataCell): Promise<Readable>;


	/** Returns all predicates exists in a category.
	 *
	 * @param category
	 */
    getAllPredicates(category: string): Promise<Readable>;



	/** Gets values specified by a tableName and an objectID.
	 * 
	 * 
	 *
	 * @param tableName
	 * @param objectID
	 * @return A Promise of Readable stream of strings.
	 * If the given table does not exist, or the given objectID is not in the table,
	 * this method returns a promise of <code>Readable.from([])</code>.
	 */
    _getValues(tableName: string, objectID: string): Promise<Readable>;


	/** Gets values specified by category, objectID, and predicate.
	 *
	 * @param cond
	 */
    getValues(cond: DataCell): Promise<Readable>;


	/** Gets a list of data cells specified by a tableName and objectID.
	 *
	 * @param tableName
	 * @param objectID
	 * @return Readable stream of DataCell objects.
	 */
    _getRows(tableName: string, objectID: string): Promise<Readable>; // DataCell[];


	/** Gets a list of data cells specified by category, objectID and predicate.
	 *
	 * @param cond
	 * @return Readable stream of DataCell objects.
	 */
    getRows(cond: DataCell): Promise<Readable>; // DataCell[];


	/** Returns an array of all data cells in a given table.
	 *
	 * This method does not check if the given table exists or not.
	 * If the given table does not exist, this method throws an exception.
	 *
	 * @param tableName
	 * @return Readable stream of DataCell objects.
	 */
    _getAllRows(tableName: string): Promise<Readable>; // DataCell[];



	/** Returns an array of all data cells in a given table (a category / predicate pair).
	 *
	 * This method does not check if the given table exists or not.
	 * If the given table does not exist, this method throws an exception.
	 *
	 * @param tableName
	 * @param objectID
	 * @return Readable stream of DataCell objects.
	 */
    getAllRows(cond: DataCell): Promise<Readable>; // DataCell[];




	/** Tests whether a given objectID is contained in a given table.
	 *
	 * @param tableName
	 * @param objectID
	 */
    _hasID(tableName: string, objectID: string): Promise<boolean>;



	/** Tests whether a given objectID is contained in a given table (a category / predicate pair).
	 *
	 * @param cond
	 */
    hasID(cond: DataCell): Promise<boolean>;


	/** Tests whether a given objectID / value pair exists or not in a given table.
	 *
	 * @param tableName
	 * @param objectID
	 * @param value
	 */
    _hasRow(tableName: string, objectID: string, value: any): Promise<boolean>;


	/** Tests whether a given objectID / value pair exists or not in a given table (category / predicate pair).
	 *
	 * @param cond
	 */
    hasRow(cond: DataCell): Promise<boolean>;


	/** Add a datacell in the data store.
	 *
	 * Existence of the given table will not be checked.
	 *
	 * @param tableName
	 * @param objectID
	 * @param value
	 */
    _addRow(tableName: string, objectID: string, value: any): Promise<void>;


	/** Add a datacell in the data store.
	 *
	 * Existence of the given table will not be checked.
	 *
	 * @param cell
	 */
    addRow(cell: DataCell): Promise<void>;


	/** Add a data cell in the data store.
	 *
	 * If the table does not exist, the table will be created.
	 *
	 * @param tableName
	 * @param objectID
	 * @param value
	 */
    _putRow(tableName: string, objectID: string, value: any): Promise<void>;


	/** Add a data cell in the data store.
	 *
	 * If the table does not exist, the table will be created.
	 *
	 * @param cell
	 */
    putRow(cell: DataCell): Promise<void>;


    _putRowIfKeyValuePairIsAbsent(tableName: string, objectID: string, value: any): Promise<void>;


    putRowIfKeyValuePairIsAbsent(cell: DataCell): Promise<void>;


    _putRowIfKeyIsAbsent(tableName: string, objectID: string, value: any): Promise<void>;


    putRowIfKeyIsAbsent(cell: DataCell): Promise<void>;


    _putRowWithReplacingValue(tableName: string, objectID: string, value: any): Promise<void>;


    putRowWithReplacingValue(cell: DataCell): Promise<void>;


    _deleteRows(tableName: string, objectID: string, value: string): Promise<void>;


    deleteRows(cond: DataCell): Promise<void>;


    _deleteID(tableName: string, objectID: string): Promise<string>;


    _isManagementTable(tableName: string): boolean;


    _categoryToObjectIDs(category: string): Promise<Readable>;


    getManagementTableNames(): string[];


    print(): Promise<void>;




}

