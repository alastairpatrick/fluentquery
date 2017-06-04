const fakeIndexedDB = require("fake-indexeddb");
const fakeIDBKeyRange = require("fake-indexeddb/lib/FDBKeyRange");

const idbbase = require("../idbbase");

idbbase.indexedDB = fakeIndexedDB
idbbase.cmp = fakeIndexedDB.cmp;
idbbase.IDBKeyRange = fakeIDBKeyRange;
