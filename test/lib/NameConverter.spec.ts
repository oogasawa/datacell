
import { expect } from 'chai';
import 'mocha';

import { NameConverter } from "../../src/lib/NameConverter";
import { DataCellStore } from "../../src/lib/DataCellStore";
import { MemDB } from "../../src/lib/MemDB";
// import { DataCellStore } from "../../src/libs/DataCellStore";

import * as log4js from "log4js";
const logger = log4js.getLogger();


describe('NameConverter', () => {

    let dbObj: DataCellStore = null;

    let nc: NameConverter = null;

    beforeEach(async () => {
        dbObj = new MemDB();
        await dbObj.connect();
        nc = dbObj.getNameConverter();
    });


    afterEach(async () => {
        await dbObj.disconnect();
    });


    context("creation of a NameConverter object", () => {
        it('A NameConverter object should be created by DataCellStore::connect() method..', async () => {

            //logger.level = "debug";
            const result: boolean = await nc.hasOriginalName("dummy");
            expect(result).to.equal(false);

            // logger.level = "error";
        });
    });


    context("_makeInternalName", () => {
        it('should return upper case string of origName when origName matches the alnum pattern.', async () => {
            const result: string = await nc._makeInternalName("alnum");
            expect(result).to.equal("ALNUM");
        });

        it('should connect words with underscores.', async () => {
            let result: string = await nc._makeInternalName("alnum abc");
            expect(result).to.equal("ALNUM_ABC");

            result = await nc._makeInternalName("alnum abc def");
            expect(result).to.equal("ALNUM_ABC_DEF");
        });

        it('should truncate string when the origName is too long.', async () => {

            const result: string = await nc._makeInternalName("this is an example of the original name which is too long");
            expect(result).to.equal("THIS_IS_AN_EXAMPLE_O00001");

        });


        it('should prefixed alnum when origName contains non-alnum characters.', async () => {
            let result: string = await nc._makeInternalName("including@nonalnum");
            expect(result).to.equal("NONALNUM00001");

            result = await nc._makeInternalName("別の例");
            expect(result).to.equal("NONALNUM00002");

        });


    });


    context("getInternalName", () => {
        it('should return internal name corresponds to the given original name.', async () => {

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
            expect(result).to.equal(intlNames[0]);

            result = await nc.getInternalName(origNames[1]);
            expect(result).to.equal(intlNames[1]);

            result = await nc.getInternalName(origNames[2]);
            expect(result).to.equal(intlNames[2]);

        });
    });


    context("getOriginalName", () => {
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
            expect(result).to.equal(origNames[0]);

            result = await nc.getOriginalName(intlNames[1]);
            expect(result).to.equal(origNames[1]);

            result = await nc.getOriginalName(intlNames[2]);
            expect(result).to.equal(origNames[2]);

        });
    });


    context("makeTableName", () => {
        it('should return table name that consists of a pair of two internal names.', async () => {

            const result: string = await nc.makeTableName("actor topic", "a too long name which should be prefixed");

            expect(result).to.equal("ACTOR_TOPIC__A_TOO_LONG_NAME_WHIC00001");
        });
    });



    context("hasOriginalName", () => {
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
            expect(result).to.equal(true);

            result = await nc.hasOriginalName("UNKNOWN_INTL_NAME");
            expect(result).to.equal(false);


        });
    });



});
