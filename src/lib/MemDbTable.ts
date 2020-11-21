

import { Readable } from "stream";


export class MemDbTable {


    entity: Map<string, string[]>;


    constructor() {
        this.entity = new Map();
    }


    get(key: string): string[] {
        return this.entity.get(key);
    }


    put(key: string, value: string): void {

        if (this.entity.has(key)) {
            this.entity.get(key).push(value);
        }
        else {
            this.entity.set(key, [value]);
        }
    }


    clear(): void {
        this.entity.clear();
    }



    isEmpty(): boolean {
        return this.entity.size === 0;
    }


    has(key: string): boolean {
        return this.entity.has(key);
    }


    keys(): IterableIterator<string> {
        return this.entity.keys();
    }



    getAllRows(): Readable {

        const table = this;
        const iter: IterableIterator<string> = this.keys();

        return new Readable({
            objectMode: true,
            read() {
                const elem: IteratorResult<string> = iter.next();
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



    removeKey(key: string): void {
        this.entity.delete(key);
    }



    set(key: string, values: string[]): void {
        this.entity.set(key, values);
    }


    size(): number {
        let counter = 0;
        for (const [key, value] of this.entity) {
            counter += value.length;
        }
        return counter;
    }


}
