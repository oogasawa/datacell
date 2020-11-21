
import lineByLine from "n-readlines";
import { Transform } from "stream";
import { MemDbTable } from "../../src/lib/MemDbTable";
import * as arraylib from "datacell-arraylib";

import * as log4js from "log4js";
const logger = log4js.getLogger();


function read_data(fname: string): object[] {

    const result: object[] = [];

    const liner = new lineByLine(fname);
    let line: false | Buffer;
    while (line = liner.next()) {
        if (line.toString().length === 0) {
            continue;
        }
        let row: string[] = line.toString().split("\t");
        result.push(row);
    }

    return result;
}




describe('MemDbTable', () => {

    describe("Construction of an empty table", () => {

        test('Construction of an empty table.', () => {
            const table = new MemDbTable();
            expect(table.isEmpty()).toEqual(true);
            expect(table.size()).toEqual(0);
        });

    });


    describe("Addition of elements", () => {

        let table: MemDbTable = null;

        beforeEach(() => {
            table = new MemDbTable();
        });

        test('#put adds DataCells with differet keys as different rows.', () => {
            table.put("A001", "value1");
            table.put("A002", "value1");
            expect(table.size()).toEqual(2);

            let value: string[] = table.get("A001");
            expect(value).toBeDefined();
            expect(value.length).toEqual(1);
            expect(value[0]).toEqual("value1");

            value = table.get("A002");
            expect(value).toBeDefined();
            expect(value.length).toEqual(1);
            expect(value[0]).toEqual("value1");

        });



        test('#put adds DataCells with the same key as different rows.', () => {
            table.put("A001", "value1");
            table.put("A001", "value2");
            expect(table.size()).toEqual(2);

            const value: string[] = table.get("A001");
            logger.debug(value);

            expect(value).toBeDefined();
            expect(value.length).toEqual(2);
            expect(value[0]).toEqual("value1");
            expect(value[1]).toEqual("value2");


        });


        test('#put adds duplicated DataCells with the same key / value as different rows.', () => {

            table.put("A001", "value1");
            table.put("A001", "value2");
            table.put("A001", "value1");
            expect(table.size()).toEqual(3);

        });

    });

    describe("Removing rows and clearing tables.", () => {

        let table: MemDbTable = null;

        beforeEach(() => {
            table = new MemDbTable();

            table.put("A001", "value1");
            table.put("A002", "value1");
            table.put("A001", "value2");
            table.put("A002", "value2");
        });


        test('#clear removes all rows in a table.', () => {

            expect(table.size()).toEqual(4);

            table.clear();
            expect(table.size()).toEqual(0);

            table.put("A001", "value1");
            table.put("A002", "value1");
            table.put("A001", "value2");

            expect(table.size()).toEqual(3);

        });


        test('#removeKey removes all rows with a given key', () => {

            expect(table.size()).toEqual(4);

            table.removeKey("A001");
            expect(table.size()).toEqual(2);

            table.removeKey("A002");
            expect(table.size()).toEqual(0);
            expect(table.isEmpty()).toBeTruthy();
        });


    });



    describe("Examination of table contents", () => {

        let table: MemDbTable = null;

        beforeEach(() => {
            table = new MemDbTable();

            table.put("A001", "value1");
            table.put("A002", "value1");
            table.put("A001", "value2");
            table.put("A002", "value2");
        });



        test('#has checkes if given key is contained in the table.', () => {
            expect(table.has("A001")).toBeTruthy();
            expect(table.has("A002")).toBeTruthy();
            expect(table.has("A003")).toBeFalsy();

        });


        test('#isEmpty checkes if the table is empty or not.', () => {
            expect(table.isEmpty()).toBeFalsy();
            table.clear();
            expect(table.isEmpty()).toBeTruthy();
        });


        test('#size returns number of rows (not number of keys) in the table.', () => {
            expect(table.size()).toEqual(4);
            table.clear();
            expect(table.size()).toEqual(0);
        });


    });


    describe("Iteration over a table (small data).", () => {

        let table: MemDbTable = null;

        beforeEach(() => {
            table = new MemDbTable();

            table.put("A001", "value1");
            table.put("A002", "value1");
            table.put("A001", "value2");
            table.put("A002", "value2");
        });



        test('#keys returns an iterator of keys.', () => {

            const result: string[] = [];

            const iter: IterableIterator<string> = table.keys();
            let elem: IteratorResult<string> = null;
            while (true) {
                elem = iter.next();
                if (elem.done) {
                    break;
                }
                result.push(elem.value);
            }

            expect(arraylib.isSubset(result, ["A001", "A002"])).toBeTruthy();
            expect(arraylib.isSubset(["A001", "A002"], result)).toBeTruthy();

        });



        test('#getAllRows returns the Readable stream containing all rows.', async () => {

            let obj = null;
            for await (obj of table.getAllRows()) {
                const { id, value } = obj;
                expect(id === "A001" || id === "A002").toBeTruthy();
                expect(value === "value1" || value === "value2").toBeTruthy();
            }

        });

    });



    describe("Iteration over a table (medium size data).", () => {

        let table: MemDbTable = null;

        beforeEach(() => {
            table = new MemDbTable();
            const data: object[] = read_data("test/data.txt");

            data.forEach((obj) => {
                const row: string[] = obj as string[];
                table.put(row[1], row[3]);
            });

        });




        test('#keys returns an iterator of keys.', () => {

            let counter = 0;

            const iter: IterableIterator<string> = table.keys(); // iterator of ids.
            let elem: IteratorResult<string> = null;
            while (true) {
                elem = iter.next();
                if (elem.done) {
                    break;
                }
                counter++;
            }

            expect(counter).toEqual(71);

        });



        test('#getAllRows returns the Readable stream containing all rows.',
            async () => {
                let counter = 0;

                let obj = null;
                for await (obj of table.getAllRows()) {
                    const { id, value } = obj;
                    counter++;
                }

                expect(counter).toEqual(198);

            });

    });


});
