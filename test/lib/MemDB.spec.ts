
import { expect } from 'chai';
import 'mocha';

import { MemDB } from "../../src/lib/MemDB";
import { DataCell } from "../../src/lib/DataCell";
import { TreeSet, Collections } from "typescriptcollectionsframework";
import * as streamlib from "datacell-streamlib";
import { Readable } from "stream";

import * as log4js from "log4js";
const logger = log4js.getLogger();
// logger.level = "debug";

const data: string[][] = [
    ["actor topic",
        "20200520-004844-473575",
        "category",
        "actor topic"],
    ["actor topic",
        "20200520-004844-473575",
        "destPath",
        "isDaemonAlive"],
    ["actor topic",
        "20200520-004844-473575",
        "name",
        "stopDaemon"],
    ["actor topic",
        "20200520-004844-473575",
        "ActorDef",
        "EShellAD"],
    ["actor topic",
        "20200520-004844-473575",
        "effect",
        "{\"pre\": undefined, \"post_ex\": false }"]
];



describe('MemDB', () => {

    let dbObj: MemDB;

    before(() => {
        dbObj = new MemDB();
    });


    beforeEach(() => {
        dbObj.connect();
    });

    afterEach(() => {
        dbObj.disconnect();
    });


    context("constructor", () => {


        it('immediately after the construction, there are three management tables in the store.', async () => {
            const tables: string[] = await streamlib.streamToArray(await dbObj.getAllTablesIncludingManagementTables());
            expect(tables.length).equals(3);
        });


        it('immediately after the construction, no tables should be contained in the store other than the management tables.', async () => {
            const tables: string[] = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(0);
        });


        it('immediately after the construction, getAllCategories() should return an empty array.', async () => {
            const categories: string[] = await streamlib.streamToArray(await dbObj.getAllCategories());
            // console.log(tables);
            expect(categories.length).to.equal(0);
        });


    });


    context("getAllTables()", () => {

        it('should be able to add a row.', async () => {
            const dc0 = new DataCell(data[0]);

            let tables: string[] = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(0);

            await dbObj.putRow(dc0);

            tables = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(1);

            const dc1 = new DataCell(data[1]);
            await dbObj.putRow(dc1);

            tables = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(2);


            const dc2 = new DataCell(data[2]);
            await dbObj.putRow(dc2);

            tables = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(3);

            await dbObj.putRow(dc2);

            tables = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(3);
            // logger.level = "debug";
            // logger.debug(tables);
            // logger.level = "error";

        });

    });


    context("getAllCategories", () => {

        it('should be able to add a row.', async () => {
            const dc0 = new DataCell(data[0]);

            let tables: string[] = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(0);

            await dbObj.putRow(dc0);

            tables = await streamlib.streamToArray(await dbObj.getAllTables());
            expect(tables.length).to.equal(1);

            const c: string[] = await streamlib.streamToArray(await dbObj.getAllCategories());
            expect(c.length).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });


    });


    context("getAllRows", () => {

        it('should return zero element stream.', async () => {
            const result: DataCell[] = [];
            const r_stream: Readable = await dbObj.getAllRows(new DataCell("dummy_category", "", "dummy_pred", ""));
            for await (const dc of r_stream) {
                result.push(dc);
            }

            expect(result.length).to.equal(0);
        });


        it('should reject addition of the same objectID / value pairs.', async () => {
            const dc1 = new DataCell(data[0]);
            await dbObj.putRow(dc1);
            await dbObj.putRow(dc1); // trying to add the same row (which should be rejected).
            await dbObj.putRow(dc1); // trying to add the same row (which should be rejected).

            // returning all rows in the "actor_topic__category" table.
            const result: DataCell[] = [];
            const r_stream: Readable = await dbObj.getAllRows(dc1);
            for await (const dc of r_stream) {
                result.push(dc);
            }

            expect(result.length).to.equal(1);
            expect(result[0].category).to.equal(data[0][0]);
            expect(result[0].objectId).to.equal(data[0][1]);
            expect(result[0].predicate).to.equal(data[0][2]);
            expect(result[0].value).to.equal(data[0][3]);

        });



        it('should accept DataCells with the same objectID and different values.', async () => {

            const dc0 = new DataCell(data[0]);
            const dc1 = new DataCell(data[1]);
            const dc2 = new DataCell(data[2]);
            const dc3 = new DataCell(data[0][0], data[0][1], data[0][2], "another value");
            await dbObj.putRow(dc0);
            await dbObj.putRow(dc1);
            await dbObj.putRow(dc2);
            await dbObj.putRow(dc3);


            // returning all rows in the "actor_topic__category" table.
            const result: DataCell[] = [];
            const r_stream: Readable = await dbObj.getAllRows(dc0);
            for await (const dc of r_stream) {
                result.push(dc);
            }

            expect(result.length).to.equal(2);
            expect(result[0].category).to.equal(data[0][0]);
            expect(result[0].objectId).to.equal(data[0][1]);
            expect(result[0].predicate).to.equal(data[0][2]);
            expect(result[0].value).to.equal(data[0][3]);
            expect(result[1].category).to.equal(data[0][0]);
            expect(result[1].objectId).to.equal(data[0][1]);
            expect(result[1].predicate).to.equal(data[0][2]);
            expect(result[1].value).to.equal("another value");


            // expect(dbObj.getAllTables().length).to.equal(3);
            // const result: DataCell[] = dbObj.getAllRows(dc1); // gets all rows in the "actor_topic__category" table.
            // expect(result.length).to.equal(2);
        });


    });


    context("putRowIfKeyIsAbsent", () => {

        it('should create table if it is absent.', async () => {
            const dc1 = new DataCell(data[0]);
            await dbObj.putRowIfKeyIsAbsent(dc1);

            const c: string[] = await streamlib.streamToArray(await dbObj.getAllCategories());
            expect(c.length).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });


        it('should reject new value when the same key exists.', async () => {

            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            const dc2 = new DataCell(data[0][0], data[0][1], data[0][2], "another value");
            await dbObj.putRowIfKeyIsAbsent(dc1);
            await dbObj.putRowIfKeyIsAbsent(dc2);

            const r_stream: Readable = await dbObj.getAllRows(dc1);
            let dc: DataCell = null;
            let counter = 0;
            for await (dc of r_stream) {
                expect(dc.category).to.equal(data[0][0]);
                expect(dc.value).to.equal(data[0][3]);
                counter++;
            }
            expect(counter).to.equal(1);
        });


    });



    context("putRowIfKeyValuePairIsAbsent", () => {


        it('should create table if it is absent.', async () => {
            const dc1 = new DataCell(data[0]);
            await dbObj.putRowIfKeyValuePairIsAbsent(dc1);

            const c: string[] = await streamlib.streamToArray(await dbObj.getAllCategories());
            expect(c.length).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });


        it('should store both values when the same key exists.', async () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            const dc2 = new DataCell(data[0][0], data[0][1], data[0][2], "another value");
            await dbObj.putRowIfKeyValuePairIsAbsent(dc1);
            await dbObj.putRowIfKeyValuePairIsAbsent(dc2);

            const r_stream: Readable = await dbObj.getAllRows(dc1); // gets all rows in the "actor_topic__category" table.
            let dc: DataCell = null;
            let counter = 0;
            const result: DataCell[] = [];
            for await (dc of r_stream) {
                result.push(dc);
                counter++;
            }
            expect(counter).to.equal(2);

            const set1: TreeSet<string> = new TreeSet<string>(Collections.getStringComparator());
            const set2: TreeSet<string> = new TreeSet<string>(Collections.getStringComparator());
            set1.add(data[0][3]);
            set1.add("another value");
            set2.add(result[0].value);
            set2.add(result[1].value);

            const msg: string = set1.toJSON().toString() + "\t" + set2.toJSON().toString();

            expect(set1.toJSON().toString() === set2.toJSON().toString(), msg).to.be.true;


        });


    });




});
