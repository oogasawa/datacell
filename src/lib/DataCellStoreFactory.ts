

import { DataCellStore } from "./DataCellStore";


export interface DataCellStoreFactory {

    getInstance(dbName: string): DataCellStore;

}
