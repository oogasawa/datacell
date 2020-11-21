
import { DataCell } from "../../src/lib/DataCell";
import { Readable, Transform } from "stream";
import lineByLine from "n-readlines";
import { sprintf } from "sprintf-js";

import * as log4js from "log4js";
const logger = log4js.getLogger();



describe('DataCell', function() {

    describe("single object", function() {
        let dc: DataCell = null;

        beforeEach(function() {
            dc = new DataCell(
                "actor topic",
                "20200520-004844-473575",
                "destPath",
                "isDaemonAlive"
            );

        });


        test('DataCell object can be constructed from four strings.', () => {
            expect(dc.category).toEqual("actor topic");
            expect(dc.objectId).toEqual("20200520-004844-473575");
            expect(dc.predicate).toEqual("destPath");
            expect(dc.value).toEqual("isDaemonAlive");
        });


        test('DataCell object can be stringify with JSON.stringify().', () => {
            const result = JSON.stringify(dc);
            const answer = JSON.stringify({
                "category": "actor topic",
                "objectId": "20200520-004844-473575",
                "predicate": "destPath",
                "value": "isDaemonAlive", "info": {}
            });

            expect(result).toEqual(answer);
            // logger.level = "debug";
            // logger.debug(JSON.stringify(dc));
            // logger.level = "error";
        });

    });


    describe("A stream of DataCells", function() {

        function read_data(fname: string): DataCell[] {

            const result: DataCell[] = [];

            const liner = new lineByLine(fname);
            let line: false | Buffer;
            while (line = liner.next()) {
                let row: string[] = line.toString().split("\t");
                result.push(new DataCell(row));
            }

            return result;
        }



        let data: DataCell[] = null;

        beforeEach(function() {
            data = read_data("./test/data.txt");
        });


        test('Get properties of each DataCell object from a stream of DataCell objects', async function() {

            // logger.level = "debug";

            let result: string[] = [];

            // --- Definition of a stream ---
            const r_stream = Readable.from(data)
                .pipe(new Transform({
                    readableObjectMode: true,
                    writableObjectMode: true,
                    transform(chunk, encode, done) {
                        let { category, objectId, predicate, value } = chunk;
                        logger.debug(sprintf("%s: %s", "category", category));
                        logger.debug(sprintf("%s: %s", "objectId", objectId));
                        logger.debug(sprintf("%s: %s", "predicate", predicate));
                        logger.debug(sprintf("%s: %s", "value", value));

                        result.push(category);
                        done();
                    }
                }));

            // --- Make the stream run ---
            for await (let obj of r_stream) {
                ; // empty statement.
            }

            // logger.level = "error";

            expect(result[0]).toEqual("feature");
            expect(result[100]).toEqual("entry");

            /* Log outputs of this test are as follows.
            [2020-11-14T23:53:10.012] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.013] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.013] [DEBUG] default - predicate: Q_organism
            [2020-11-14T23:53:10.013] [DEBUG] default - value: "Paracoccus sp. AK26"
            [2020-11-14T23:53:10.013] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.013] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.013] [DEBUG] default - predicate: Q_mol_type
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "genomic DNA"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_strain
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "AK26"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_isolation_source
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "Hexachlorocyclohexane (HCH)\npesticide-contaminated soil"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_db_xref
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "taxon:2589076"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_country
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "India: Lucknow, UP"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"source","range":"1..2787975"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_collection_date
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "2015"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"gene","range":"463..1314"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_locus_tag
            [2020-11-14T23:53:10.014] [DEBUG] default - value: "FIU66_00005"
            [2020-11-14T23:53:10.014] [DEBUG] default - category: feature
            [2020-11-14T23:53:10.014] [DEBUG] default - objectId: {"accession":"CP041041","feature":"CDS","range":"463..1314"}
            [2020-11-14T23:53:10.014] [DEBUG] default - predicate: Q_locus_tag
            [2020-11-14T23:53:10.015] [DEBUG] default - value: "FIU66_00005"
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041041
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: definition
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26 chromosome, complete genome.
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041041
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: organism
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041042
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: definition
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26 plasmid pAK4, complete sequence.
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041042
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: organism
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041043
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: definition
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26 plasmid pAK3, complete sequence.
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041043
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: organism
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041044
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: definition
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26 plasmid pAK2, complete sequence.
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041044
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: organism
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26
            [2020-11-14T23:53:10.015] [DEBUG] default - category: entry
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: CP041045
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: definition
            [2020-11-14T23:53:10.015] [DEBUG] default - value: Paracoccus sp. AK26 plasmid pAK1, complete sequence.
            [2020-11-14T23:53:10.015] [DEBUG] default - category: 
            [2020-11-14T23:53:10.015] [DEBUG] default - objectId: undefined
            [2020-11-14T23:53:10.015] [DEBUG] default - predicate: undefined
            [2020-11-14T23:53:10.016] [DEBUG] default - value: undefined
            */

        });



    });


});


