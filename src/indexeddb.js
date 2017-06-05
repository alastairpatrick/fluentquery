"use strict";

const { Observable } = require("./rx");

const { IDBKeyRange } = require("./idbbase");
const { Range, includes } = require("./range");
const { traversePath } = require("./traverse");
const { Table } = require("./tree");

const has = Object.prototype.hasOwnProperty;
const identity = x => x;

const idbRange = (range) => {
  if (range.lower === undefined && range.upper === undefined)
    return null;
  else if (range.lower !== undefined && range.upper !== undefined)
    return IDBKeyRange.bound(range.lower, range.upper, range.lowerOpen, range.upperOpen);
  else if (range.lower !== undefined)
    return IDBKeyRange.lowerBound(range.lower, range.lowerOpen);
  else
    return IDBKeyRange.upperBound(range.upper, range.upperOpen);
}

const rangeStream = (source, range) => {
  return Observable.create(observer => {
    let request = source.openCursor(idbRange(range));
    request.onsuccess = function(event) {
      let cursor = event.target.result;
      if (cursor) {
        cursor.continue();
        observer.next(cursor.value);
      } else {
        observer.complete();
      }
    };
    request.onerror = function(event) {
      observer.error(event.target.error);
    };
  });
}

const keyPathSetterMemo = Object.create(null);
const keyPathSetter = (source) => {
  if (source.keyPath === null) {
    return identity;
  } else if (typeof source.keyPath == "string") {
    let setter = keyPathSetterMemo[source.keyPath];
    if (setter)
      return setter;

    let keyPath = source.keyPath.split(".");
    let keyPathJS = keyPath.reduce((js, k) => js + '[' + JSON.stringify(k) + ']', "tuple");
    setter = new Function("tuple", "key", keyPathJS + " = key");

    keyPathSetterMemo[source.keyPath] = setter;
    return setter;
  }
}

class IDBTable extends Table {
  constructor(db, name) {
    super();

    if (typeof db !== "object")
      throw new Error("Expected IDBDatabase");
    if (typeof name !== "string")
      throw new Error("Expected table name");

    this.db = db;
    this.name = name;
  }

  chooseBestIndex(store, keyRanges) {
    let range;
    let index;

    let indicesByKeyPath = Object.create(null);
    indicesByKeyPath[store.keyPath] = store;

    if (keyRanges !== undefined) {
      // Primary key is always best.
      if (typeof store.keyPath === "string" && has.call(keyRanges, store.keyPath)) {
        return {
          range: keyRanges[store.keyPath],
          index: store,
        }
      }

      for (let i = 0; i < store.indexNames.length; ++i) {
        let n = store.indexNames[i];
        let idx = store.index(n);
        if (typeof idx.keyPath === "string" && !idx.multiEntry && has.call(keyRanges, idx.keyPath)) {
            range = keyRanges[idx.keyPath];
            index = idx;
        }
      }

      // Prefer unique index
      for (let i = 0; i < store.indexNames.length; ++i) {
        let n = store.indexNames[i];
        let idx = store.index(n);
        if (typeof idx.keyPath === "string" && !idx.multiEntry && idx.unique && has.call(keyRanges, idx.keyPath)) {
            range = keyRanges[idx.keyPath];
            index = idx;
        }
      }
    }

    return {
      range,
      index,
    };
  }

  execute(context, keyRanges) {
    let store = context.transaction.objectStore(this.name);
    let best = this.chooseBestIndex(store, keyRanges);

    if (best.range === undefined) {
      return rangeStream(store, new Range(undefined, undefined));
    } else {
      let observable = Observable.empty();
      let ranges = best.range.prepare(context);
      for (let i = 0; i < ranges.length; ++i)
        observable = observable.concat(rangeStream(best.index, ranges[i]));

      return observable;
    }
  }

  put(context, tuples, options) {
    options = Object.assign({
      overwrite: true,
    }, options);

    let store = context.transaction.objectStore(this.name);
    let method = options.overwrite ? store.put.bind(store) : store.add.bind(store);

    let setKeyPath = keyPathSetter(store);

    let requests = [];
    for (let i = 0; i < tuples.length; ++i) {
      let request = method(tuples[i]);

      // This error handler will be replaced when something subscribes to the observable.
      request.onerror = function(event) {
        throw event.target.error;
      }
      
      requests.push(request);
    };

    if (requests.length === 0)
      return Observable.from([]);

    if (store.autoIncrement && store.keyPath !== null) {
      // In this path, the onsuccess callback provides the value of the auto-generated key.
      return Observable.create(observer => {
        tuples.forEach((tuple, i) => {
          let request = requests[i];

          if (i < tuples.length - 1) {
            request.onsuccess = function(event) {
              setKeyPath(tuple, event.target.result);
              observer.onNext(tuple);
            };
          } else {
            request.onsuccess = function(event) {
              setKeyPath(tuple, event.target.result);
              observer.next(tuple);
              observer.complete();
            };
          }

          request.onerror = function(event) {
            observer.error(event.target.error);
          };
        });
      });
    } else {
      // In this path, only need to watch for the last onsuccess callback being called.
      return Observable.create(observer => {
        for (let i = 0; i < requests.length; ++i) {
          let request = requests[i];
          request.onerror = function(event) {
            observer.error(event.target.error);
          };
        };

        requests[requests.length - 1].onsuccess = function(event) {
          for (let i = 0; i < tuples.length; ++i) {
            let tuple = tuples[i];
            observer.next(tuple);
          }
          observer.complete();
        };
      });
    }
  }

  tree() {
    return {
      class: this.constructor.name,
    };
  }
}

class IDBTransaction {
  constructor(relation, db, tableNames, mode) {
    this.relation = relation;
    this.db = db;
    this.tableNames = tableNames;
    this.mode = mode;
  }

  execute(context) {
    context.db = this.db;
    if (context.transaction === undefined)
      context.transaction = this.db.transaction(Array.from(this.tableNames), this.mode);
    return context.execute(this.relation);
  }

  tree() {
    return {
      class: this.constructor.name,
      tableNames: Array.from(this.tableNames).sort(),
      relation: this.relation.tree(),
      mode: this.mode,
    };
  }
}

module.exports = {
  IDBTable,
  IDBTransaction,
};
