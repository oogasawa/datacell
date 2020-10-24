
import { sprintf } from "sprintf-js";
import { DataCellStore } from "./DataCellStore";
import * as streamlib from "datacell-streamlib";

import * as log4js from "log4js";
const logger = log4js.getLogger();


export class NameConverter {

    originalName: string;
    internalName: string;

    store: DataCellStore;


    alnumPattern = /^[A-Za-z0-9_ ]+$/;
    prefixPattern = /[A-Za-z0-9_]+/;
    tableNamePattern = /^([A-Z0-9_]+?)__([A-Z0-9_]+)$/;


    MAX_LENGTH_OF_PREFIX = 20;
    NUMBER_OF_DIGITS = 5;
    NAME_PREFIX = "NONALNUM";


    managementTableNames: string[] = [
        "ORIGINAL_NAME__INTERNAL_NAME",
        "INTERNAL_NAME__ORIGINAL_NAME",
        "INTERNAL_NAME__MAX_COUNT"
    ];



    constructor() {
        // nothing to do.
    }


    async init(store: DataCellStore): Promise<void> {
        this.store = store;
        await this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
        await this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
        await this.store._createTable("INTERNAL_NAME__MAX_COUNT");
    }





	/** Set a data cell store to this object and creates management tables.
	 *
	 * @category Store Access
	 *
	 * @param store
	 */
    async setDataCellStore(store: DataCellStore): Promise<void> {
        this.store = store;
        await this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
        await this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
        await this.store._createTable("INTERNAL_NAME__MAX_COUNT");
    }



	/** Tests whether the given internal name is stored in the NameConverter store.
	 *
	 * @category Store Access
	 * @param internalName
	 */
    async hasOriginalName(internalName: string): Promise<boolean> {
        const result: string[]
            = await streamlib.streamToArray(
                await this.store._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName));

        return result.length > 0;
    }


	/** Returns original name stored in the management table.
	 * 
	 * @category Store Access
	 * @param internalName
	 */
    async getOriginalName(internalName: string): Promise<string> {
        const result: string[]
            = await streamlib.streamToArray(
                await this.store._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName));

        return result[0];
    }


	/** Returns the internal name corresponds to the given orig name.
	 * 
	 * If the internal name exists in the datacell store, 
	 * this method returns the name.
	 * Otherwise, this method creates a new internal name
	 * and returns it.
	 *
	 * @category Store Access
	 * @param origName  - an original name.
	 * @return internal name.
	 */
    async getInternalName(origName: string): Promise<string> {

        const result: string[]
            = await streamlib.streamToArray(
                await this.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origName));

        if (result.length >= 1) {
            return result[0];
        }
        else {
            const internalName: string
                = await this._makeInternalName(origName);
            await this.setInternalName(origName, internalName);
            return internalName;
        }

    }


	/** Stores a internalName / originalName pair to the database.
	 *
	 * @category Store Access
	 * @param origName
	 * @param internalName
	 */
    async setInternalName(origName: string, internalName: string): Promise<void> {
        await this.store._putRowWithReplacingValue("INTERNAL_NAME__ORIGINAL_NAME", internalName, origName);
        await this.store._putRowWithReplacingValue("ORIGINAL_NAME__INTERNAL_NAME", origName, internalName);
    }


	/** Returns internal name string corresponding to the given origName.
	 *
	 * This method access to the management tables 
	 * by calling getCount() method which return counter of the prefix.
	 *
	 * @category Store Acess
	 * @param origName - an original name.
	 * @return generated internal name
	 */
    async _makeInternalName(origName: string): Promise<string> {

        let internalName: string;
        let prefix: string;
        let counter = 0;

        const m = this.alnumPattern.exec(origName);
        if (m !== null) { // match to the alnum pattern.
            if (origName.length <= this.MAX_LENGTH_OF_PREFIX) {
                internalName = origName.split(" ").join("_").toUpperCase();
            }
            else {
                prefix = origName.substring(0, this.MAX_LENGTH_OF_PREFIX);
                prefix = prefix.split(" ").join("_").toUpperCase(); // replace all " " with "_".
                counter = await this.getCount(prefix);
                internalName = prefix + sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
                await this.setCount(prefix, counter);
            }
        }
        else { // do not match the alnum pattern.
            prefix = this.NAME_PREFIX;
            counter = await this.getCount(prefix);
            internalName = prefix + sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
            await this.setCount(prefix, counter);
        }

        // logger.debug("NameConverter::_makeInternalName() : origName = " + origName);
        // logger.debug("NameConverter::_makeInternalName() : internalName = " + internalName);

        return internalName;
    }


	/** Returns the count of given prefix.
	 *
	 * @category Store Access
	 * @param prefix
	 */
    async getCount(prefix: string): Promise<number> {
        let count = 0;
        const cList: string[]
            = await streamlib.streamToArray(
                await this.store._getValues("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix));

        if (cList.length > 0) {
            count = parseInt(cList[0], 10);
        }

        return count;
    }


	/** Set count of given prefix.
	 * 
	 * @category Store Access
	 * @param prefix
	 * @param count
	 */
    async setCount(prefix: string, count: number): Promise<void> {
        await this.store._putRowWithReplacingValue("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix, `${count}`);
    }



	/** Returns a table name (a pair of internal names joined with "__").
	 *
	 * This method use the management table for getting internal names.
	 *
	 * @category Store Access
	 * @param category
	 * @param predicate
	 */
    async makeTableName(category: string, predicate: string): Promise<string> {
        let c = await this.getInternalName(category);
        let p = await this.getInternalName(predicate);
        return c + "__" + p;
    }



	/** Converts a table name to a pair of category name and predicate.
	 *
	 * This method do not access to the management tables.
	 *
	 * @param tableName - a table name.
	 * @return a pair of internal names.
	 * If the given string do not match the table name pattern,
	 * this method returns a zero length array (that is, <code>[]</code>).
	 */
    parseTableName(tableName: string): string[] {
        const result: string[] = [];
        const m = this.tableNamePattern.exec(tableName);

        if (m !== null) {
            result.push(m[1].toUpperCase());
            result.push(m[2].toUpperCase());
        }

        return result;
    }


	/** Check if the given table name string matches the table name pattern.
	 *
	 * This method do not access to the management tables.
	 *
	 * @param tableName
	 */
    isValidTableName(tableName: string): boolean {
        let m = this.tableNamePattern.exec(tableName);
        return m !== null;
    }



}
