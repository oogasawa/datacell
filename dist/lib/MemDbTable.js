"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MemDbTable = void 0;
const stream_1 = require("stream");
class MemDbTable {
    constructor() {
        this.entity = new Map();
    }
    get(key) {
        return this.entity.get(key);
    }
    put(key, value) {
        if (this.entity.has(key)) {
            this.entity.get(key).push(value);
        }
        else {
            this.entity.set(key, [value]);
        }
    }
    clear() {
        this.entity.clear();
    }
    isEmpty() {
        return this.entity.size === 0;
    }
    has(key) {
        return this.entity.has(key);
    }
    keys() {
        return this.entity.keys();
    }
    getAllRows() {
        const table = this;
        const iter = this.keys();
        return new stream_1.Readable({
            objectMode: true,
            read() {
                const elem = iter.next();
                if (elem.done) {
                    this.push(null);
                }
                else {
                    const k = elem.value;
                    const values = table.get(k);
                    values.forEach((v) => {
                        this.push({ id: k, value: v });
                    });
                }
            }
        });
    }
    removeKey(key) {
        this.entity.delete(key);
    }
    set(key, values) {
        this.entity.set(key, values);
    }
    size() {
        let counter = 0;
        for (const [key, value] of this.entity) {
            counter += value.length;
        }
        return counter;
    }
}
exports.MemDbTable = MemDbTable;
