"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NameConverter = void 0;
var sprintf_js_1 = require("sprintf-js");
var NameConverter = /** @class */ (function () {
    function NameConverter() {
        this.alnumPattern = /^[A-Za-z0-9_ ]+$/;
        this.prefixPattern = /[A-Za-z0-9_]+/;
        this.tableNamePattern = /^([A-Z0-9_]+?)__([A-Z0-9_]+)$/;
        this.MAX_LENGTH_OF_PREFIX = 20;
        this.NUMBER_OF_DIGITS = 5;
        this.NAME_PREFIX = "NONALNUM";
        this.managementTableNames = [
            "ORIGINAL_NAME__INTERNAL_NAME",
            "INTERNAL_NAME__ORIGINAL_NAME",
            "INTERNAL_NAME__MAX_COUNT"
        ];
        // nothing to do.
    }
    NameConverter.prototype.init = function (store) {
        this.store = store;
        this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
        this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
        this.store._createTable("INTERNAL_NAME__MAX_COUNT");
    };
    /** Set a data cell store to this object and creates management tables.
     *
     * @category Store Access
     *
     * @param store
     */
    NameConverter.prototype.setDataCellStore = function (store) {
        this.store = store;
        this.store._createTable("ORIGINAL_NAME__INTERNAL_NAME");
        this.store._createTable("INTERNAL_NAME__ORIGINAL_NAME");
        this.store._createTable("INTERNAL_NAME__MAX_COUNT");
    };
    /** Tests whether the given internal name is stored in the NameConverter store.
     *
     * @category Store Access
     * @param internalName
     */
    NameConverter.prototype.hasOriginalName = function (internalName) {
        var result = this.store
            ._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName);
        return result.length > 0;
    };
    /** Returns original name stored in the management table.
     *
     * @category Store Access
     * @param internalName
     */
    NameConverter.prototype.getOriginalName = function (internalName) {
        var result = this.store
            ._getValues("INTERNAL_NAME__ORIGINAL_NAME", internalName);
        return result[0];
    };
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
    NameConverter.prototype.getInternalName = function (origName) {
        var result = this.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origName);
        if (result.length >= 1) {
            return result[0];
        }
        else {
            var internalName = this._makeInternalName(origName);
            this.setInternalName(origName, internalName);
            return internalName;
        }
    };
    /** Stores a internalName / originalName pair to the database.
     *
     * @category Store Access
     * @param origName
     * @param internalName
     */
    NameConverter.prototype.setInternalName = function (origName, internalName) {
        this.store._putRowWithReplacingValue("INTERNAL_NAME__ORIGINAL_NAME", internalName, origName);
        this.store._putRowWithReplacingValue("ORIGINAL_NAME__INTERNAL_NAME", origName, internalName);
    };
    /** Returns internal name string corresponding to the given origName.
     *
     * This method access to the management tables
     * by calling getCount() method which return counter of the prefix.
     *
     * @category Store Acess
     * @param origName - an original name.
     * @return generated internal name
     */
    NameConverter.prototype._makeInternalName = function (origName) {
        var internalName;
        var prefix;
        var counter = 0;
        var m = this.alnumPattern.exec(origName);
        if (m !== null) { // match to the alnum pattern.
            if (origName.length <= this.MAX_LENGTH_OF_PREFIX) {
                internalName = origName.split(" ").join("_").toUpperCase();
            }
            else {
                prefix = origName.substring(0, this.MAX_LENGTH_OF_PREFIX);
                prefix = prefix.split(" ").join("_").toUpperCase(); // replace all " " with "_".
                counter = this.getCount(prefix);
                internalName = prefix + sprintf_js_1.sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
                this.setCount(prefix, counter);
            }
        }
        else { // do not match the alnum pattern.
            prefix = this.NAME_PREFIX;
            counter = this.getCount(prefix);
            internalName = prefix + sprintf_js_1.sprintf("%0" + this.NUMBER_OF_DIGITS + "d", ++counter);
            this.setCount(prefix, counter);
        }
        return internalName;
    };
    /** Returns the count of given prefix.
     *
     * @category Store Access
     * @param prefix
     */
    NameConverter.prototype.getCount = function (prefix) {
        var count = 0;
        var cList = this.store._getValues("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix);
        if (cList.length > 0) {
            count = parseInt(cList[0], 10);
        }
        return count;
    };
    /** Set count of given prefix.
     *
     * @category Store Access
     * @param prefix
     * @param count
     */
    NameConverter.prototype.setCount = function (prefix, count) {
        this.store._putRowWithReplacingValue("INTERNAL_NAME_PREFIX__MAX_COUNT", prefix, "" + count);
    };
    /** Returns a table name (a pair of internal names joined with "__").
     *
     * This method use the management table for getting internal names.
     *
     * @category Store Access
     * @param category
     * @param predicate
     */
    NameConverter.prototype.makeTableName = function (category, predicate) {
        var c = this.getInternalName(category);
        var p = this.getInternalName(predicate);
        return c + "__" + p;
    };
    /** Converts a table name to a pair of category name and predicate.
     *
     * This method do not access to the management tables.
     *
     * @param tableName - a table name.
     * @return a pair of internal names.
     * If the given string do not match the table name pattern,
     * this method returns a zero length array (that is, <code>[]</code>).
     */
    NameConverter.prototype.parseTableName = function (tableName) {
        var result = [];
        var m = this.tableNamePattern.exec(tableName);
        if (m !== null) {
            result.push(m[1].toUpperCase());
            result.push(m[2].toUpperCase());
        }
        return result;
    };
    /** Check if the given table name string matches the table name pattern.
     *
     * This method do not access to the management tables.
     *
     * @param tableName
     */
    NameConverter.prototype.isValidTableName = function (tableName) {
        var m = this.tableNamePattern.exec(tableName);
        return m !== null;
    };
    return NameConverter;
}());
exports.NameConverter = NameConverter;
