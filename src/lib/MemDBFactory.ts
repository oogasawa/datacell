
import { DataCellStore } from "./DataCellStore";
import { DataCellStoreFactory } from "./DataCellStoreFactory";
import { MemDB } from "./MemDB";



export class MemDBFactory implements DataCellStoreFactory {

    getInstance(dbName: string): DataCellStore {
        return new MemDB();
    }


    // readFile(fname: string): DataCellStore {
    //     return this._pass2(fname, this._pass1(fname));
    // }


    // /** Gathers category info. */
    // _pass1(fname: string): HashMap<string, string> {

    //     const categoryInfo = new HashMap<string, string>();

    //     // read data from a file.
    //     const liner = new lineByLine(fname);
    //     let line: boolean | Buffer;

    //     while (line = liner.next()) {

    //         const record = csvSync(line, {
    //             skip_empty_lines: true,
    //             trim: true
    //         });

    //         if (record[0][1] === "category") { // get category info.

    //             // console.log(record[0]);
    //             if (typeof record[0][2] === "object") {
    //                 categoryInfo.put(record[0][0], JSON.parse(record[0][2]));
    //             }

    //             else if (typeof record[0][2] === "string") {
    //                 categoryInfo.put(record[0][0], record[0][2]);
    //             }
    //         }

    //     }

    //     return categoryInfo;
    // }




    // /** Read data and join to the category info. */
    // _pass2(fname: string, categoryInfo: HashMap<string, string>): MemDB {

    //     const mem = new MemDB();

    //     // read data from a file.
    //     const liner = new lineByLine(fname);
    //     let line: boolean | Buffer;


    //     while (line = liner.next()) {

    //         const record = csvSync(line, {
    //             skip_empty_lines: true,
    //             trim: true
    //         });


    //         if (record[0][0] === "") { // skip empty lines.
    //             continue;
    //         }


    //         const category = categoryInfo.get(record[0][0]);

    //         if (typeof record[0][2] === "object") {

    //             mem.putRowIfKeyValuePairIsAbsent(
    //                 new DataCell(category,
    //                     record[0][0],
    //                     record[0][1],
    //                     JSON.parse(record[0][2])));
    //         }
    //         else if (typeof record[0][2] === "string") {
    //             mem.putRowIfKeyValuePairIsAbsent(
    //                 new DataCell(category,
    //                     record[0][0],
    //                     record[0][1],
    //                     record[0][2]));
    //         }

    //     }

    //     return mem;
    // }



}
