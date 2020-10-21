
import { expect } from 'chai';
import 'mocha';

import { DataCell } from "../../src/lib/DataCell";


describe('DataCell', () => {

    context("constructor", () => {
        it('should create a data cell.', () => {
            const dc = new DataCell(
                "actor topic",
                "20200520-004844-473575",
                "destPath",
                "isDaemonAlive"
            );
            // console.log(dc);
            // expect(result).to.equal(false);
        });
    });


});
