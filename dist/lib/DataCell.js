"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DataCell = void 0;
const sync_1 = __importDefault(require("csv-parse/lib/sync"));
// https://stackoverflow.com/questions/9804777/how-to-test-if-a-string-is-json-or-not
function isJson(str) {
    try {
        JSON.parse(str);
    }
    catch (e) {
        return false;
    }
    return true;
}
class DataCell {
    constructor(category, objectId, predicate, value) {
        if (typeof (category) === "string") {
            this.category = category;
            this.objectId = objectId;
            this.predicate = predicate;
            this.value = value;
        }
        else if (typeof (category) === "object") {
            this.category = category[0];
            this.objectId = category[1];
            this.predicate = category[2];
            this.value = category[3];
        }
        this.info = {};
    }
    encodeTSV() {
        const result = [];
        result.push(this.category);
        result.push(this.objectId);
        result.push(this.predicate);
        result.push(JSON.stringify(this.value));
        return result.join("\t");
    }
    decodeTSV(line) {
        const record = sync_1.default(line, {
            delimiter: "\t",
            trim: true
        });
        let cell;
        if (isJson(record[0][3])) {
            cell = new DataCell(record[0][0], record[0][1], record[0][2], JSON.parse(record[0][3]));
        }
        else {
            cell = new DataCell(record[0][0], record[0][1], record[0][2], record[0][3]);
        }
        return cell;
    }
}
exports.DataCell = DataCell;
