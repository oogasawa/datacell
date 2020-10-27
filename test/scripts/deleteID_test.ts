

import * as log4js from "log4js";
const logger = log4js.getLogger();
// logger.level = "debug";

class UniqueKVMap<K, V> {
    entity: Map<K, Set<V>>;

    constructor() {
        this.entity = new Map();
    }


    get(key: K): Set<V> {
        return this.entity.get(key);
    }


    put(key: K, value: V): void {

        if (this.entity.has(key)) {
            const vs: Set<V> = this.entity.get(key);
            if (vs.has(value)) {
                return;
            }
            else {
                this.entity.get(key).add(value);
            }
        }
        else { // this map does not have the given key.
            const valList = new Set<V>();
            valList.add(value);
            this.entity.set(key, valList);
        }
    }


    clear(): void {
        this.entity.clear();
    }


    containsKey(key: K): boolean {
        return this.entity.has(key);
    }



    getValues(key: K): V[] {
        const result: V[] = [];
        const set: Set<V> = this.get(key);

        set.forEach((elem) => {
            result.push(elem);
        });

        return result;
    }


    isEmpty(): boolean {
        return this.entity.size === 0;
    }


    keySet(): Set<K> {
        const result = new Set<K>();
        this.entity.forEach((v, k, m) => {
            result.add(k);
        });
        return result;
    }


    removeKey(key: K): void {
        this.entity.delete(key);
    }


    size(): number {
        return this.entity.size;
    }


}



main();



function direct_test() {

    const data = new UniqueKVMap<string, string>();
    data.put("20200520-004844-473575", "EShellAD");
    data.put("20200520-004844-473575", "EShellAD2");
    data.put("20200101-000000-000000", "InterpreterAD");

    data.keySet().forEach((k) => {
        console.log(k);
    });


    data.removeKey("20200520-004844-473575");

    data.keySet().forEach((k) => {
        console.log(k);
    });

}


async function main() {

    direct_test();

    // const dbObj: MemDB = new MemDB();
    // dbObj.connect();

    // await dbObj._putRow("ACTOR_TOPIC__ACTORDEF", "20200520-004844-473575", "EShellAD");
    // await dbObj._putRow("ACTOR_TOPIC__ACTORDEF", "20200520-004844-473575", "EShellAD2");
    // await dbObj._putRow("ACTOR_TOPIC__ACTORDEF", "20200101-000000-000000", "InterpreterAD");

    // let result: string[] = await streamlib.streamToArray(await dbObj._getIDs("ACTOR_TOPIC__ACTORDEF"));
    // logger.level = "debug";
    // logger.debug("_putRow and _deleteID : " + JSON.stringify(result));


    // let out: string = await dbObj._deleteID("ACTOR_TOPIC__ACTORDEF", "20200520-004844-473575");
    // console.log(out);
    // // result = await streamlib.streamToArray(await dbObj._getIDs("ACTOR_TOPIC__ACTORDEF"));
    // // logger.debug("_putRow and _deleteID : " + JSON.stringify(result));

    // // logger.level = "error";

    // dbObj.disconnect();
}


