
import { Comparator, TreeSet } from "typescriptcollectionsframework";




main();

function main() {
    let obj = new Map<string, string[]>();

    obj.set("one", ["ichi", "ni", "san"]);
    obj.set("two", ["ni", "ni", "san"]);


    console.log(obj);
}
