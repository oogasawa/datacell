import * as streamlib from "datacell-streamlib";
import * as arraylib from "datacell-arraylib";
import { DataCell } from "../../src/lib/DataCell";
import { NameConverter } from "../../src/lib/NameConverter";
import { DataCellStore } from "../../src/lib/DataCellStore";
import { MemDB } from "../../src/lib/MemDB";
import { sprintf } from "sprintf-js";
import { Readable, Transform } from "stream";
// import { DataCellStore } from "../../src/libs/DataCellStore";

import * as log4js from "log4js";
const logger = log4js.getLogger();


const maintainanceTables = [
    "ORIGINAL_NAME__INTERNAL_NAME",
    "INTERNAL_NAME__ORIGINAL_NAME",
    "INTERNAL_NAME__MAX_COUNT"
];


async function print_maintainance_tables(dbObj: DataCellStore, context: string) {

    for (let t of maintainanceTables) {

        let r_stream: Readable = await dbObj._getAllRows(t);
        r_stream
            .pipe(new Transform({
                readableObjectMode: true,
                writableObjectMode: true,
                transform(chunk, encode, done) {
                    let { category, objectId, predicate, value } = chunk;
                    this.push(JSON.stringify(new DataCell(category, objectId, predicate, value)));
                    done();
                }
            }));


        for await (let dc of r_stream) {
            let objStr: string = JSON.stringify(dc);
            logger.debug(sprintf("NameConverter.spec.ts  %s -- %s: %s", context, t, objStr));
        }

    }

}


describe('NameConverter', () => {

    let dbObj: DataCellStore = null;

    let nc: NameConverter = null;



    describe("Creation of a NameConverter object in a dbObj (here, a MemDB object)", () => {

        beforeEach(async () => {
            dbObj = new MemDB();
            await dbObj.connect();
            nc = dbObj.getNameConverter();
        });

        afterEach(async () => {
            await dbObj.disconnect();
        });


        test('A NameConverter object is created in MemDB.', async () => {
            expect(dbObj.getNameConverter().constructor.name).toEqual("NameConverter");
        });


        test('Management tables are created in MemDB.', () => {
            maintainanceTables.forEach((t) => {
                expect(dbObj._hasTable(t)).toBeTruthy();
            });
        });


        test('Management tables contains original name/internal name of their own.', async () => {
            // check if all the management tables are still empty after calling getCount("nonexistent").
            // logger.level = "debug";
            let rows: string[] = [];
            rows = await streamlib.streamToArray(await dbObj._getAllRows(maintainanceTables[0]));
            expect(rows.length).toEqual(3); // "ORIGINAL_NAME", "INTERNAL_NAME", "MAX_COUNT"
            rows = await streamlib.streamToArray(await dbObj._getAllRows(maintainanceTables[1]));
            expect(rows.length).toEqual(3); // "ORIGINAL_NAME", "INTERNAL_NAME", "MAX_COUNT"
            rows = await streamlib.streamToArray(await dbObj._getAllRows(maintainanceTables[2]));
            expect(rows.length).toEqual(0); // There is no "prefix" for the names which do not match alnum pattern. 
        });


        test('#getCount("nonexistent") should return 0.', async function() {

            async function experiment(origName: string) {
                const result = await dbObj.getNameConverter().getCount(origName);
                expect(result).toEqual(0);

            }

            // Between before and after calling getCount("nonexistent"),
            // the maintainance tables should be unchanged. 
            await experiment("nonexistent");  // alnum name.
            await experiment("non-existent"); // non-alnum name.

        });



        test('#getInternalName("something") should return an internal name.', async function() {

            // --- Test Definition. ---
            async function experiment(name: string, answer: string, count: number) {

                const result = await dbObj.getNameConverter().getInternalName(name);
                expect(result).toEqual(answer);
                // logger.debug("#getInternalName, result: " + result);

                const origName = await dbObj.getNameConverter().getOriginalName(result);
                expect(origName).toEqual(name);
                // logger.debug("#getInternalName, result => origName: " + origName);

                let c1 = await dbObj.getNameConverter().getCount("NONALNUM");
                logger.debug("#getInternalName, getCount: NONALNUM: " + count);
                expect(c1).toEqual(count);
                let c2 = await dbObj.getNameConverter().getCount("NONEXISTENT");
                expect(c2).toEqual(0);
                // logger.debug("#getInternalName, getCount: NONEXISTENT: " + count);



                // check the contents of the management tables.
                for (let t of maintainanceTables) {
                    let result: DataCell[] = [];
                    const r_stream: Readable = await dbObj._getAllRows(t);
                    const r_stream2 = r_stream
                        .pipe(new Transform({
                            readableObjectMode: true,
                            writableObjectMode: true,
                            transform(chunk, encode, done) {
                                let { category, objectId, predicate, value } = chunk;
                                logger.debug(sprintf("%s, %s: %s", "#getInternalName", t, objectId));
                                done();
                            }
                        }));

                    for await (let o of r_stream2) {
                        ;
                    }

                }


            }

            // --- Test execution with different parameters. ---
            //logger.level = "debug";
            await experiment("nonexistent", "NONEXISTENT", 0);
            await experiment("nonexistent", "NONEXISTENT", 0);
            await experiment("non-existent", "NONALNUM00001", 1);
            await experiment("non-existent2", "NONALNUM00002", 2);
            logger.level = "error";
        });

    });




    describe("Test #_makeInternalName", () => {

        beforeEach(async function() {
            dbObj = new MemDB();
            await dbObj.connect();
            nc = dbObj.getNameConverter();
        });

        afterEach(async function() {
            await dbObj.disconnect();
        });


        test('Processing normal alnum pattern.', async function() {
            let result: string = await nc._makeInternalName("alnum");
            expect(result).toEqual("ALNUM");

            result = await nc._makeInternalName("alnum abc");
            expect(result).toEqual("ALNUM_ABC");

            result = await nc._makeInternalName("alnum abc def");
            expect(result).toEqual("ALNUM_ABC_DEF");
        });

        test('Truncation of long names.', async () => {

            const result: string = await nc._makeInternalName("this is an example of the original name which is too long");
            expect(result).toEqual("THIS_IS_AN_EXAMPLE_O00001");

        });


        test('Processing names cotaining non-alnum characters.', async () => {
            let result: string = await nc._makeInternalName("including@nonalnum");
            expect(result).toEqual("NONALNUM00001");

            result = await nc._makeInternalName("別の例");
            expect(result).toEqual("NONALNUM00002");

        });

    });



    describe("getInternalName", () => {
        it('#getInternalName should return internal name corresponds to the given original name.', async () => {

            const origNames = ["actor topic",
                "an too long name which should be prefixed",
                "日本語"]
            const intlNames: string[] = [];
            intlNames.push(await nc._makeInternalName(origNames[0]));
            intlNames.push(await nc._makeInternalName(origNames[1]));
            intlNames.push(await nc._makeInternalName(origNames[2]));

            await nc.setInternalName(origNames[0], intlNames[0]);
            await nc.setInternalName(origNames[1], intlNames[1]);
            await nc.setInternalName(origNames[2], intlNames[2]);

            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[0]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[1]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[2]));

            let result: string = await nc.getInternalName(origNames[0]);
            expect(result).toEqual(intlNames[0]);

            result = await nc.getInternalName(origNames[1]);
            expect(result).toEqual(intlNames[1]);

            result = await nc.getInternalName(origNames[2]);
            expect(result).toEqual(intlNames[2]);

        });
    });


    describe("getOriginalName", () => {
        it('should return original name corresponds to the given internal name.', async () => {

            const origNames = ["actor topic",
                "an too long name which should be prefixed",
                "日本語"]
            const intlNames: string[] = [];
            intlNames.push(await nc._makeInternalName(origNames[0]));
            intlNames.push(await nc._makeInternalName(origNames[1]));
            intlNames.push(await nc._makeInternalName(origNames[2]));

            await nc.setInternalName(origNames[0], intlNames[0]);
            await nc.setInternalName(origNames[1], intlNames[1]);
            await nc.setInternalName(origNames[2], intlNames[2]);

            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[0]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[1]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[2]));

            let result: string = await nc.getOriginalName(intlNames[0]);
            expect(result).toEqual(origNames[0]);

            result = await nc.getOriginalName(intlNames[1]);
            expect(result).toEqual(origNames[1]);

            result = await nc.getOriginalName(intlNames[2]);
            expect(result).toEqual(origNames[2]);

        });
    });


    describe("Test #makeTableName", () => {
        it('should return table name that consists of a pair of two internal names.', async () => {

            const result: string = await nc.makeTableName("actor topic", "a too long name which should be prefixed");

            expect(result).toEqual("ACTOR_TOPIC__A_TOO_LONG_NAME_WHIC00001");
        });
    });



    describe("hasOriginalName", () => {
        it('should return true or false depending on whether the original name is stored or not.', async () => {

            const origNames = ["actor topic",
                "an too long name which should be prefixed",
                "日本語"]
            const intlNames: string[] = [];
            intlNames.push(await nc._makeInternalName(origNames[0]));
            intlNames.push(await nc._makeInternalName(origNames[1]));
            intlNames.push(await nc._makeInternalName(origNames[2]));

            await nc.setInternalName(origNames[0], intlNames[0]);
            await nc.setInternalName(origNames[1], intlNames[1]);
            await nc.setInternalName(origNames[2], intlNames[2]);

            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[0]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[1]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[2]));

            let result: boolean = await nc.hasOriginalName(intlNames[0]);
            expect(result).toEqual(true);

            result = await nc.hasOriginalName("UNKNOWN_INTL_NAME");
            expect(result).toEqual(false);


        });
    });



    describe("#_report() method", () => {
        it('should print out all rows of the management tables.', async () => {

            const origNames = ["actor topic",
                "an too long name which should be prefixed",
                "日本語"]
            const intlNames: string[] = [];
            intlNames.push(await nc._makeInternalName(origNames[0]));
            intlNames.push(await nc._makeInternalName(origNames[1]));
            intlNames.push(await nc._makeInternalName(origNames[2]));

            await nc.setInternalName(origNames[0], intlNames[0]);
            await nc.setInternalName(origNames[1], intlNames[1]);
            await nc.setInternalName(origNames[2], intlNames[2]);

            logger.level = "debug";
            // await nc._report("debug");
            logger.level = "error";

            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[0]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[1]));
            // console.log(nc.store._getValues("ORIGINAL_NAME__INTERNAL_NAME", origNames[2]));

            // let result: string = await nc.getInternalName(origNames[0]);
            // expect(result).toEqual(intlNames[0]);

            // result = await nc.getInternalName(origNames[1]);
            // expect(result).toEqual(intlNames[1]);

            // result = await nc.getInternalName(origNames[2]);
            // expect(result).toEqual(intlNames[2]);



        });
    });


});
