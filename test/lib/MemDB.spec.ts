
import lineByLine from "n-readlines";
import { MemDB } from "../../src/lib/MemDB";
import { DataCell } from "../../src/lib/DataCell";
import * as streamlib from "datacell-streamlib";
import { Readable, Transform, Writable } from "stream";


import * as log4js from "log4js";
const logger = log4js.getLogger();
// logger.level = "debug";




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


describe('MemDB', () => {

    let dbObj: MemDB;

    beforeAll(function() {
        dbObj = new MemDB();
    });


    beforeEach(async function() {
        await dbObj.connect();
    });

    afterEach(async function() {
        await dbObj.disconnect();
    });


    describe("immediately after construction", function() {

        afterEach(async function() {
            await dbObj.deleteAllTables();
        });


        test('#getAllTables should return an empty array.', async function() {
            const tables: string[] = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).toEqual(0);
        });


        test('#getAllCategories() should return an empty array.', async function() {
            const categories: string[] = await streamlib.streamToArray(await dbObj.getAllCategories());
            expect(categories.length).toEqual(0);
        });


        test('#getNameConverter() should return the NameConverter object associated with the dbObj.', function() {
            const result = dbObj.getNameConverter();
            expect(result.constructor.name).toEqual("NameConverter");
        });


    });


    describe("putting rows (small data)", () => {

        let data: object[] = [];

        beforeEach(async function() {

            // Reads data from a file into an object array.
            data = read_data("test/data.txt");
        });


        afterEach(async function() {
            await dbObj.deleteAllTables();
        });


        test('#_addRow should be able to create duplicated data rows.', async () => {

            const dc = new DataCell(data[0]);
            const tableName = await dbObj.getNameConverter().makeTableName(data[0][0], data[0][2]);
            await dbObj._createTable(tableName);
            await dbObj._addRow(tableName, data[0][1], data[0][3]);
            await dbObj._addRow(tableName, data[0][1], data[0][3]);

            // logger.level = "debug";

            let count = 0;
            let rst: Readable = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());

            for await (let dc of rst) {
                count++;
                logger.debug("#_addRow test : " + JSON.stringify(dc));
            }
            expect(count).toEqual(2);

            logger.level = "error";
        });


        test('#addRow : DataCell version of #_addRow', async () => {

            const dc = new DataCell(data[0]);
            const tableName = await dbObj.getNameConverter()
                .makeTableName(data[0][0], data[0][2]);
            await dbObj._createTable(tableName);
            await dbObj.addRow(new DataCell(data[0]));
            await dbObj.addRow(new DataCell(data[0]));

            // logger.level = "debug";

            let count = 0;
            let rst: Readable = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                count++;
                logger.debug("#_addRow test : " + JSON.stringify(dc));
            }
            expect(count).toEqual(2);

            logger.level = "error";
        });



        test('#putRow should be able to create duplicated data rows.', async () => {

            // Load a row into the array with duplication.
            await dbObj.putRow(new DataCell(data[0]));
            await dbObj.putRow(new DataCell(data[0]));

            let count = 0;
            const dc = new DataCell(data[0]);
            let rst: Readable = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                count++;
            }
            expect(count).toEqual(2);

        });


        test('#putRowIfKeyIsAbsent', async () => {

            // Load a row into the array with duplication.
            await dbObj.putRowIfKeyIsAbsent(new DataCell(data[0]));
            await dbObj.putRowIfKeyIsAbsent(new DataCell(data[0]));

            let count = 0;
            const dc = new DataCell(data[0]);
            let rst: Readable = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                count++;
            }
            expect(count).toEqual(1);

            await dbObj.putRowIfKeyIsAbsent(
                new DataCell(data[0][0],
                    data[0][1],
                    data[0][2],
                    "another value"));


            count = 0;
            let result: string = null;
            rst = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                const { category, objectId, predicate, value } = dc;
                result = value;
                count++;
            }
            expect(count).toEqual(1);
            expect(result).toEqual(data[0][3]);

        });


        test('#putRowIfKeyValuePairIsAbsent', async () => {

            // Load a row into the array with duplication.
            await dbObj.putRowIfKeyValuePairIsAbsent(new DataCell(data[0]));
            await dbObj.putRowIfKeyValuePairIsAbsent(new DataCell(data[0]));

            let count = 0;
            const dc = new DataCell(data[0]);
            let rst: Readable = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                count++;
            }
            expect(count).toEqual(1);

            await dbObj.putRowIfKeyValuePairIsAbsent(
                new DataCell(data[0][0],
                    data[0][1],
                    data[0][2],
                    "another value"));


            count = 0;
            let result: string = null;
            rst = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                const { category, objectId, predicate, value } = dc;
                result = value;
                count++;
            }
            expect(count).toEqual(2);
            expect(result).toEqual("another value");

        });



        test('#putRowWithReplacingValue', async () => {

            // Load a row into the array with duplication.
            await dbObj.putRowWithReplacingValue(new DataCell(data[0]));
            await dbObj.putRowWithReplacingValue(new DataCell(data[0]));

            let count = 0;
            const dc = new DataCell(data[0]);
            let rst: Readable = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                count++;
            }
            expect(count).toEqual(1);

            await dbObj.putRowWithReplacingValue(
                new DataCell(data[0][0],
                    data[0][1],
                    data[0][2],
                    "another value"));


            count = 0;
            let result: string = null;
            rst = await dbObj.getAllRows(dc);
            rst.pipe(new streamlib.DevNull());
            for await (let dc of rst) {
                const { category, objectId, predicate, value } = dc;
                result = value;
                count++;
            }
            expect(count).toEqual(1);
            expect(result).toEqual("another value");

        });


    });



    describe("Iteration over a table", () => {

        let data: object[] = [];

        beforeEach(async function() {

            // Reads data from a file into an object array.
            data = read_data("test/data.txt");
            for (let row of data) {
                logger.debug("#putRow: row = " + row);
                await dbObj.putRow(new DataCell(row));
            }
        });


        afterEach(async function() {
            await dbObj.deleteAllTables();
        });



        afterEach(async function() {
            await dbObj.deleteAllTables();
        });


        test('#_getAllRows should return all rows in a table', async () => {

            // logger.level = "debug";
            const tableName: string
                = await dbObj.getNameConverter()
                    .makeTableName(data[0][0], data[0][2]);


            async function experiment(tableName, rowNum) {

                // Definition of a Readable stream.
                // let categorySet = new Set<string>();
                // let objectIdSet = new Set<string>();
                // let predicateSet = new Set<string>();
                const rst: Readable = await dbObj._getAllRows(tableName);
                let counter = 0;
                rst
                    .pipe(new Transform({
                        readableObjectMode: true,
                        writableObjectMode: true,
                        transform(chunk, encode, done) {
                            counter++;
                            const { category, objectId, predicate, value } = chunk;
                            // categorySet.add(category);
                            // objectIdSet.add(objectId);
                            // predicateSet.add(predicate);
                            // this.push(chunk);
                            done();
                        }
                    }))
                    .pipe(new streamlib.DevNull());

                // Making the stream run.
                let counter2 = 0;
                let obj = null;
                for await (obj of rst) {
                    counter2++;
                }

                // expect(categorySet.size).toEqual(1);
                // expect(objectIdSet.size).toEqual(1);
                // expect(predicateSet.size).toEqual(1);
                expect(counter).toEqual(rowNum);
                expect(counter2).toEqual(rowNum);
            }
            logger.level = "error";


            await experiment("FEATURE__Q_ORGANISM", 1);
            await experiment("FEATURE__Q_LOCUS_TAG", 20);


        });



        test('#getAllRows is the DataCell version of #_getAllRows', async () => {

            // logger.level = "debug";
            const tableName: string
                = await dbObj.getNameConverter()
                    .makeTableName(data[0][0], "Q_locus_tag");

            // Definition of a Readable stream.
            let categorySet = new Set<string>();
            let objectIdSet = new Set<string>();
            let predicateSet = new Set<string>();
            const rst: Readable = await dbObj.getAllRows(
                new DataCell(data[0][0], "", "Q_locus_tag", ""));
            rst
                .pipe(new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform(chunk, encode, done) {
                        const { category, objectId, predicate, value } = chunk;
                        categorySet.add(category);
                        objectIdSet.add(objectId);
                        predicateSet.add(predicate);
                        // this.push(chunk);
                        done();
                    },
                    flush(done) {
                        done();
                    }
                }))
                .pipe(new streamlib.DevNull());

            // Making the stream run.
            let obj = null;
            for await (obj of rst) {
                ;
            }

            expect(categorySet.size).toEqual(1);
            expect(objectIdSet.size).toEqual(20);
            expect(predicateSet.size).toEqual(1);

            logger.level = "error";

        });



        test('#getAllCategories returns all categories in the database', async () => {

            const c: string[] = await streamlib.streamToArray(
                await dbObj.getAllCategories());

            expect(c.length).toEqual(2);
            expect(c[0]).toEqual("feature");
            expect(c[1]).toEqual("entry");

        });



        test('#_getIDs returns all IDs in a table', async () => {

            async function experiment(tableName, count) {

                // Definition of a Readable stream.
                let objectIdSet = new Set<string>();
                const rst: Readable = await dbObj._getIDs(tableName);
                rst
                    .pipe(new Transform({
                        readableObjectMode: true,
                        writableObjectMode: true,
                        highWaterMark: 16,
                        transform(chunk, encode, done) {
                            const str = chunk.toString();
                            objectIdSet.add(str);
                            this.push(chunk);
                            done();
                        }
                    }))
                    .pipe(new streamlib.DevNull());


                // Making the stream run.
                let obj = null;
                for await (obj of rst) {
                    ;
                }

                // Testing the result.
                // logger.level = "debug";
                // logger.debug(objectIdSet);
                // logger.level = "error";

                expect(objectIdSet.size).toEqual(count);


            }


            // logger.level = "debug";
            const tableName: string
                = await dbObj.getNameConverter()
                    .makeTableName(data[0][0], data[0][2]);

            await experiment(tableName, 1);
            await experiment("FEATURE__Q_LOCUS_TAG", 20);

        });


        test('#_getPredicates returns all predicates related to an objectId', async () => {

            logger.level = "debug";
            async function experiment(category, objectIds, count) {

                // Definition of a Readable stream.
                let predicateSet = new Set<string>();
                const rst: Readable
                    = await dbObj._getPredicates(category, objectIds);

                let counter = 0;
                rst
                    .pipe(new Transform({
                        readableObjectMode: true,
                        writableObjectMode: true,
                        highWaterMark: 16,
                        transform(chunk, encode, done) {
                            const str = chunk.toString();
                            predicateSet.add(str);
                            counter++;
                            this.push(chunk);
                            done();
                        }
                    }))
                    .pipe(new streamlib.DevNull());


                // Making the stream run.
                let obj = null;
                for await (obj of rst) {
                    ;
                }

                // Testing the result.
                logger.level = "debug";
                logger.debug(predicateSet);

                expect(predicateSet.size).toEqual(count);
            }



            await experiment("feature", JSON.stringify({ "accession": "CP020762", "feature": "source", "range": "1..2546158" }), 10);
            logger.level = "error";

        });



    });




});




