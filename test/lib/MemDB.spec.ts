
import { expect } from 'chai';
import 'mocha';

import { MemDB } from "../../src/lib/MemDB";
import { DataCell } from "../../src/lib/DataCell";
import { TreeSet, Collections } from "typescriptcollectionsframework";


import * as log4js from "log4js";
const logger = log4js.getLogger();
logger.level = "debug";

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



    context("constructor", () => {

        let dbObj: MemDB;

        beforeEach(() => {
            dbObj = new MemDB();
        });


        afterEach(() => {
            dbObj.close();
        });



        it('immediately after the construction, no tables should be contained in the store.', () => {
            let tables = dbObj.getAllTables();
            expect(tables.length).to.equal(0);

            tables = dbObj.getAllTablesIncludingManagementTables();
            expect(tables.length).equals(3);
            // logger.debug(tables);
        });


        it('immediately after the construction, no categories should be contained.', () => {
            const categories = dbObj.getAllCategories();
            // console.log(tables);
            expect(categories.length).to.equal(0);
        });

    });


    context("putRow", () => {

        let dbObj: MemDB;

        beforeEach(() => {
            dbObj = new MemDB();
        });


        afterEach(() => {
            dbObj.close();
        });



        it('should be able to add a row.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);

            let tables = dbObj.getAllTables();
            expect(tables.length).to.equal(0);
            // logger.debug(tables);

            dbObj.putRow(dc1);

            const c = dbObj.getAllCategories();
            const result = c.length;
            expect(result).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });


        it('should reject addition of the same objectID / value pairs.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            dbObj.putRow(dc1);
            dbObj.putRow(dc1); // trying to add the same row (which should be rejected).
            dbObj.putRow(dc1); // trying to add the same row (which should be rejected).
            const result: DataCell[] = dbObj.getAllRows(dc1); // returning all rows in the "actor_topic__category" table.
            expect(result.length).to.equal(1);
        });


        it('should accept DataCells with the same objectID and different values.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            const dc2 = new DataCell(data[1][0], data[1][1], data[1][2], data[1][3]);
            const dc3 = new DataCell(data[2][0], data[2][1], data[2][2], data[2][3]);
            const dc4 = new DataCell(data[0][0], data[0][1], data[0][2], "another value");
            dbObj.putRow(dc1);
            dbObj.putRow(dc2);
            dbObj.putRow(dc3);
            dbObj.putRow(dc4);

            expect(dbObj.getAllTables().length).to.equal(3);
            const result: DataCell[] = dbObj.getAllRows(dc1); // gets all rows in the "actor_topic__category" table.
            expect(result.length).to.equal(2);
        });


    });



    context("putRow", () => {

        let dbObj: MemDB;

        beforeEach(() => {
            dbObj = new MemDB();
        });


        afterEach(() => {
            dbObj.close();
        });



        it('should create table if it is absent.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            dbObj.putRow(dc1);

            const c = dbObj.getAllCategories();
            const result = c.length;
            expect(result).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });

    });


    context("putRowIfKeyIsAbsent", () => {

        let dbObj: MemDB;

        beforeEach(() => {
            dbObj = new MemDB();
        });


        afterEach(() => {
            dbObj.close();
        });


        it('should create table if it is absent.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            dbObj.putRowIfKeyIsAbsent(dc1);

            const c = dbObj.getAllCategories();
            const result = c.length;
            expect(result).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });


        it('should reject new value when the same key exists.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            const dc2 = new DataCell(data[0][0], data[0][1], data[0][2], "another value");
            dbObj.putRowIfKeyIsAbsent(dc1);
            dbObj.putRowIfKeyIsAbsent(dc2);


            let result: string[] = dbObj.getValues(dc1);
            expect(result.length).to.equal(1);
            expect(result[0]).to.equal(data[0][3]);
        }
        );


    });



    context("putRowIfKeyValuePairIsAbsent", () => {

        let dbObj: MemDB;

        beforeEach(() => {
            dbObj = new MemDB();
        });


        afterEach(() => {
            dbObj.close();
        });



        it('should create table if it is absent.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            dbObj.putRowIfKeyValuePairIsAbsent(dc1);

            const c = dbObj.getAllCategories();
            const result = c.length;
            expect(result).to.equal(1);
            expect(c[0]).to.equal("actor topic");

        });


        it('should store both values when the same key exists.', () => {
            const dc1 = new DataCell(data[0][0], data[0][1], data[0][2], data[0][3]);
            const dc2 = new DataCell(data[0][0], data[0][1], data[0][2], "another value");
            dbObj.putRowIfKeyValuePairIsAbsent(dc1);
            dbObj.putRowIfKeyValuePairIsAbsent(dc2);

            const result: DataCell[] = dbObj.getAllRows(dc1); // gets all rows in the "actor_topic__category" table.
            expect(result.length).to.equal(2);

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
