
import csvSync from "csv-parse/lib/sync";


// https://stackoverflow.com/questions/9804777/how-to-test-if-a-string-is-json-or-not
function isJson(str: string): boolean {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}


export class DataCell {

    category: string;

    objectId: string;

    predicate: string;

    value: any;


    info: any;


    constructor(category: string,
        objectId: string,
        predicate: string,
        value: any) {

        this.category = category;
        this.objectId = objectId;
        this.predicate = predicate;
        this.value = value;

        this.info = {};
    }



    encodeTSV(): string {
        const result: any[] = [];
        result.push(this.category);
        result.push(this.objectId);
        result.push(this.predicate);
        result.push(JSON.stringify(this.value));

        return result.join("\t");
    }



    decodeTSV(line: string): DataCell {
        const record = csvSync(line, {
            delimiter: "\t",
            trim: true
        });


        let cell: DataCell;
        if (isJson(record[0][3])) {
            cell = new DataCell(
                record[0][0],
                record[0][1],
                record[0][2],
                JSON.parse(record[0][3])
            );
        }
        else {
            cell = new DataCell(
                record[0][0],
                record[0][1],
                record[0][2],
                record[0][3]
            );
        }
        return cell;
    }


}




