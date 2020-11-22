
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

    beforeAll(() => {
        dbObj = new MemDB();
    });


    beforeEach(async () => {
        await dbObj.connect();
    });

    afterEach(async () => {
        await dbObj.disconnect();
    });


    describe("immediately after construction", () => {

        afterEach(async () => {
            await dbObj.deleteAllTables();
        });


        test('#getAllTables should return an empty array.', async () => {
            const tables: string[] = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).toEqual(0);
        });


        test('#getAllCategories() should return an empty array.', async () => {
            const categories: string[] = await streamlib.streamToArray(await dbObj.getAllCategories());
            expect(categories.length).toEqual(0);
        });


        test('#getNameConverter() should return the NameConverter object associated with the dbObj.', () => {
            const result = dbObj.getNameConverter();
            expect(result.constructor.name).toEqual("NameConverter");
        });


    });


    describe("putting rows (small data)", () => {

        let data: object[] = [];


        beforeEach(() => {
            // Reads data from a file into an object array.
            data = read_data("test/data.txt");
        });


        afterEach(async () => {
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
                // logger.debug("#putRow: row = " + row);
                await dbObj.putRow(new DataCell(row));
            }
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

            // logger.level = "debug";

            async function experiment(category, objectId, count) {

                // Definition of a Readable stream.
                let predicateSet = new Set<string>();
                const rst: Readable
                    = await dbObj._getPredicates(category, objectId);

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
                            logger.debug("#_getPredicates: " + JSON.stringify(chunk));
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
                expect(counter).toEqual(count);
                expect(predicateSet.size).toEqual(count);
            }

            await experiment("feature", JSON.stringify({ "accession": "CP020762", "feature": "source", "range": "1..2546158" }), 10);
            logger.level = "error";

        });


        test('#_getValues returns values related to given objectId in a table.', async () => {
            // logger.level = "debug";

            async function experiment(tableName, objectId, count) {

                // Definition of a Readable stream.
                const rst: Readable
                    = await dbObj._getValues(tableName, objectId);

                let counter = 0;
                rst
                    .pipe(new Transform({
                        readableObjectMode: true,
                        writableObjectMode: true,
                        highWaterMark: 16,
                        transform(chunk, encode, done) {
                            counter++;
                            logger.debug("#_getValues: " + JSON.stringify(chunk));
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
                expect(counter).toEqual(count);
            }

            await experiment("FEATURE__Q_MOL_TYPE",
                JSON.stringify({ "accession": "CP020762", "feature": "source", "range": "1..2546158" }), 1);
            logger.level = "error";


        });


        test('#_getRows is almost the same meaning of #_getValues: #_getRows returns values in DataCell form.', async () => {
            // logger.level = "debug";

            async function experiment(tableName, objectId, count) {

                // Definition of a Readable stream.
                const rst: Readable
                    = await dbObj._getRows(tableName, objectId);

                let counter = 0;
                rst
                    .pipe(new Transform({
                        readableObjectMode: true,
                        writableObjectMode: true,
                        highWaterMark: 16,
                        transform(chunk, encode, done) {
                            counter++;
                            logger.debug("#_getRows: " + JSON.stringify(chunk));
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
                expect(counter).toEqual(count);
            }

            await experiment("FEATURE__Q_MOL_TYPE",
                JSON.stringify({ "accession": "CP020762", "feature": "source", "range": "1..2546158" }), 1);
            logger.level = "error";


        });


        test('#getAllCategories returns all categories in the database', async () => {

            const c: string[] = await streamlib.streamToArray(
                await dbObj.getAllCategories());

            expect(c.length).toEqual(2);
            expect(c[0]).toEqual("feature");
            expect(c[1]).toEqual("entry");

        });



        test("#getAllPredicates returns all predicates in a category", async () => {

            // logger.level = "debug";
            async function experiment(category: string, count: number) {

                const result: string[] = await streamlib.streamToArray(await dbObj.getAllPredicates(category));

                for (let pred of result) {
                    logger.debug(pred);
                }

                expect(result.length).toEqual(count);
            }

            await experiment("feature", 18);
            await experiment("entry", 2);

            logger.level = "error";
        });


        test("#getAllPredicates : dealing with streamlib.streamToDevNull", async () => {

            // logger.level = "debug";
            async function experiment(category: string, count: number) {

                const rst: Readable = await dbObj.getAllPredicates(category);

                let counter = 0;
                const rst2 = rst.pipe(streamlib.getAsyncMap(1, async (elem) => {
                    counter++;
                    return elem;
                }));
                await streamlib.streamToDevNull(rst2);

                expect(counter).toEqual(count);
            }

            await experiment("feature", 18);
            await experiment("entry", 2);

            logger.level = "error";
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



        test("#getAllTables should return all tables", async () => {
            const rst: Readable = await dbObj.getAllTables();

            // logger.level = "debug";

            let counter = 0;
            rst.pipe(new Transform({
                readableObjectMode: true,
                writableObjectMode: true,
                transform(chunk, encode, done) {
                    counter++;
                    this.push(chunk);
                    logger.debug(chunk);
                    done();
                }
            })).pipe(new streamlib.DevNull());

            let obj = null;
            for await (obj of rst) {
                ;
            }

            expect(counter).toEqual(20);

            // logger.level = "error";
        });



    });



    describe("Getting data", () => {

        let data: object[] = [];

        beforeEach(async function() {

            // Reads data from a file into an object array.
            data = read_data("test/data.txt");
            for (let row of data) {
                // logger.debug("#putRow: row = " + row);
                await dbObj.putRow(new DataCell(row));
            }
        });


        afterEach(async function() {
            await dbObj.deleteAllTables();
        });



        test("#_getIDs : (tableName) => objectId", async () => {

            async function experiment(category: string, predicate: string, count: number) {

                const tableName: string = await dbObj.getNameConverter().makeTableName(category, predicate);
                const rst: Readable = await dbObj._getIDs(tableName);

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform(chunk, encode, done) {
                        counter++;
                        this.push(chunk);
                        logger.debug(chunk);
                        done();
                    }
                })).pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    ;
                }

                expect(counter).toEqual(count);
            }


            await experiment("feature", "Q_locus_tag", 20);
            await experiment("feature", "Q_inference", 10);
            await experiment("feature", "Q_organism", 1);
            // await experiment("feature", "nonexistent", 0);

            // logger.level = "error";
        });



        test("#getIDs : (category, predicate) => objectId: DataCell version", async () => {

            async function experiment(category: string, predicate: string, count: number) {

                const rst: Readable = await dbObj.getIDs(
                    new DataCell(category, "", predicate, ""));

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform(chunk, encode, done) {
                        counter++;
                        this.push(chunk);
                        logger.debug(chunk);
                        done();
                    }
                })).pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    ;
                }

                expect(counter).toEqual(count);
            }


            await experiment("feature", "Q_locus_tag", 20);
            await experiment("feature", "Q_inference", 10);
            await experiment("feature", "Q_organism", 1);
            // await experiment("feature", "nonexistent", 0);

            // logger.level = "error";
        });


        test("#_getPredicates: (category, objectId) => predicate", async () => {

            async function experiment(category: string, objID: string, count: number) {

                const rst: Readable = await dbObj._getPredicates(category, objID);

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform(chunk, encode, done) {
                        counter++;
                        this.push(chunk);
                        logger.debug("#getPredicates : " + chunk);
                        done();
                    }
                })).pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    ;
                }

                expect(counter).toEqual(count);
            }

            let objID = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });
            await experiment("feature", objID, 8);
            objID = JSON.stringify({ "accession": "CP020762", "feature": "gene", "range": "complement(6398..7501)" });
            await experiment("feature", objID, 1);


            // logger.level = "error";
        });



        test("#getPredicates : (category, objectId) => predicate : DataCell version.", async () => {

            async function experiment(category: string, objID: string, count: number) {

                const rst: Readable = await dbObj.getPredicates(
                    new DataCell(category, objID, "", ""));

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform(chunk, encode, done) {
                        counter++;
                        this.push(chunk);
                        logger.debug("#getPredicates : " + chunk);
                        done();
                    }
                })).pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    ;
                }

                expect(counter).toEqual(count);
            }


            let objID = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });
            await experiment("feature", objID, 8);
            objID = JSON.stringify({ "accession": "CP020762", "feature": "gene", "range": "complement(6398..7501)" });
            await experiment("feature", objID, 1);

            // logger.level = "error";
        });






        test("#_getRows : (tableName, objectId) => DataCell : DataCell version of getValues", async () => {

            async function experiment(category: string, objID: string, predicate: string, count: number) {

                const tableName: string = await dbObj.getNameConverter().makeTableName(category, predicate);
                const rst: Readable = await dbObj._getRows(tableName, objID);

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    counter++;
                }

                expect(counter).toEqual(count);
            }


            let objID = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });
            await experiment("feature", objID, "Q_locus_tag", 1);


            // logger.level = "error";
        });



        test("#getRows : (category, objectId, predicate) => DataCell : DataCell version of getValues", async () => {

            async function experiment(category: string, objID: string, predicate: string, count: number) {


                const rst: Readable = await dbObj.getRows(
                    new DataCell(category, objID, predicate, ""));

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    counter++;
                }

                expect(counter).toEqual(count);
            }


            let objID = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });
            await experiment("feature", objID, "Q_locus_tag", 1);


            // logger.level = "error";
        });




        test("#_getValues : (tableName, objectId) => value", async () => {

            async function experiment(category: string, objID: string, predicate: string, count: number) {

                const tableName: string = await dbObj.getNameConverter().makeTableName(category, predicate);
                const rst: Readable = await dbObj._getValues(tableName, objID);

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    counter++;
                }

                expect(counter).toEqual(count);
            }


            let objID = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });
            await experiment("feature", objID, "Q_locus_tag", 1);


            // logger.level = "error";
        });



        test("#getValues : (category, objectId, predicate) => value", async () => {

            async function experiment(category: string, objID: string, predicate: string, count: number) {


                const rst: Readable = await dbObj.getValues(
                    new DataCell(category, objID, predicate, ""));

                // logger.level = "debug";

                let counter = 0;
                rst.pipe(new streamlib.DevNull());

                let obj = null;
                for await (obj of rst) {
                    counter++;
                }

                expect(counter).toEqual(count);
            }


            let objID = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });
            await experiment("feature", objID, "Q_locus_tag", 1);


            // logger.level = "error";
        });



    });



    describe("Testing data", () => {

        let data: object[] = [];

        beforeEach(async function() {

            // Reads data from a file into an object array.
            data = read_data("test/data.txt");
            for (let row of data) {
                // logger.debug("#putRow: row = " + row);
                await dbObj.putRow(new DataCell(row));
            }
        });


        afterEach(async function() {
            await dbObj.deleteAllTables();
        });



        test("#_hasID : (tableName, objectId) => Promise<boolean>", async () => {

            let tableName: string = await dbObj.getNameConverter().makeTableName("feature", "Q_locus_tag");
            let objID: string = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });

            expect(await dbObj._hasID(tableName, objID)).toBeTruthy();
            expect(await dbObj._hasID(tableName, "nonexistent")).toBeFalsy();

            // logger.level = "error";
        });


        test("#hasID : (category, objectId, predicate) => Promise<boolean> : DataCell version of #_hasID", async () => {

            let objID: string = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });

            expect(await dbObj.hasID(new DataCell("feature", objID, "Q_locus_tag", ""))).toBeTruthy();
            expect(await dbObj.hasID(new DataCell("feature", "nonexistent", "Q_locus_tag", ""))).toBeFalsy();

            // logger.level = "error";
        });


        test("#_hasRow : (tableName, objectId, value) => Promise<boolean>", async () => {

            let tableName: string = await dbObj.getNameConverter().makeTableName("feature", "Q_locus_tag");
            let objID: string = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });


            // await streamlib.streamToArray(await dbObj._getValues(tableName, objID));

            expect(await dbObj._hasRow(tableName, objID, '"B7466_00045"')).toBeTruthy();
            expect(await dbObj._hasRow(tableName, objID, "nonexistent")).toBeFalsy();
            expect(await dbObj._hasRow(tableName, "nonexistent", "B7466_00045")).toBeFalsy();
            expect(await dbObj._hasRow(tableName, "nonexistent", "nonexistent")).toBeFalsy();


            // logger.level = "error";
        });



        test("#hasRow : (category, objectId, predicate, value) => Promise<boolean>", async () => {

            let tableName: string = await dbObj.getNameConverter().makeTableName("feature", "Q_locus_tag");
            let objID: string = JSON.stringify({ "accession": "CP020762", "feature": "CDS", "range": "complement(6398..7501)" });



            expect(await dbObj.hasRow(new DataCell("feature", objID, "Q_locus_tag", '"B7466_00045"'))).toBeTruthy();
            expect(await dbObj.hasRow(new DataCell("feature", objID, "Q_locus_tag", 'nonexistent'))).toBeFalsy();
            expect(await dbObj.hasRow(new DataCell("feature", "nonexistent", "Q_locus_tag", '"B7466_00045"'))).toBeFalsy();
            expect(await dbObj.hasRow(new DataCell("feature", "nonexistent", "Q_locus_tag", 'nonexistent'))).toBeFalsy();
            expect(await dbObj.hasRow(new DataCell("nonexistent", "nonexistent", "Q_locus_tag", 'nonexistent'))).toBeFalsy();


            // logger.level = "error";
        });


        test("#_hasTable ", async () => {

            let tableName: string = await dbObj.getNameConverter().makeTableName("feature", "Q_locus_tag");
            expect(await dbObj._hasTable(tableName)).toBeTruthy();
            expect(await dbObj._hasTable("FEATURE__NONEXISTENT")).toBeFalsy();

            tableName = await dbObj.getNameConverter().makeTableName("feature", "nonexistent");
            expect(await dbObj._hasTable(tableName)).toBeFalsy();

            // logger.level = "error";
        });


        test("#hasTable ", async () => {
            expect(await dbObj.hasTable(new DataCell("feature", "", "Q_locus_tag", ""))).toBeTruthy();
            expect(await dbObj.hasTable(new DataCell("feature", "", "nonexistent", ""))).toBeFalsy();
        });



    });



});

