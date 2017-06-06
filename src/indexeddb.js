"use strict";

const { Observable } = require("./rx");

const { IDBKeyRange } = require("./idbbase");
const { Range, includes } = require("./range");
const { traversePath } = require("./traverse");
const { PrimaryKey } = require("./expression");
const { Table } = require("./tree");

const has = Object.prototype.hasOwnProperty;
let identity = x => x;

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

const rangeStream = (source, idbRange) => {
  let keyPath = source.keyPath;
  return Observable.create(observer => {
    let request = source.openCursor(idbRange);
    request.onsuccess = function(event) {
      let cursor = event.target.result;
      if (cursor) {
        cursor.continue();
        if (keyPath === null)
          cursor.value[PrimaryKey] = cursor.primaryKey;
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

const getKeyPaths = (source) => {
  if (source.keyPath === null) {
    return { keyPaths: [] };
  } else if (typeof source.keyPath == "string") {
    return { keyPaths: [source.keyPath], array: false };
  } else if (Array.isArray(source.keyPath)) {
    return { keyPaths: source.keyPath, array: true };
  } else {
    throw new Error(`Key path not supported: '${source.keyPath}'.`);
  }
}

const keyPathSetterMemo = Object.create(null);
keyPathSetterMemo["null"] = (tuple, key) => tuple[PrimaryKey] = key;

const keyPathSetter = (source) => {
  let keyPathKey = JSON.stringify(source.keyPath);
  let setter = keyPathSetterMemo[keyPathKey];
  if (setter)
    return setter;

  let { keyPaths, array } = getKeyPaths(source);
  let js = "";
  keyPaths.forEach((keyPath, i) => {
    let keyPathMemberExpr = keyPath.split(".").reduce((js, k) => js + '[' + JSON.stringify(k) + ']', "tuple");
    if (array)
      js += `${keyPathMemberExpr} = key[${i}];\n`;
    else
      js += `${keyPathMemberExpr} = key;\n`;
  });

  setter = new Function("tuple", "key", js);

  keyPathSetterMemo[keyPathKey] = setter;
  return setter;
}

const keyPathGetterMemo = Object.create(null);
keyPathGetterMemo["null"] = (tuple) => tuple[PrimaryKey];

const keyPathGetter = (source) => {
  let keyPathKey = JSON.stringify(source.keyPath);
  let getter = keyPathGetterMemo[keyPathKey];
  if (getter)
    return getter;

  let { keyPaths, array } = getKeyPaths(source);
  let js = "";
  let sep = "";
  keyPaths.forEach((keyPath, i) => {
    let keyPathMemberExpr = keyPath.split(".").reduce((js, k) => js + '[' + JSON.stringify(k) + ']', "tuple");
    js += sep + keyPathMemberExpr;
    sep = ", ";
  });

  if (array)
    js = "[" + js + "]";
  
  getter = new Function("tuple", "return " + js);

  keyPathGetterMemo[keyPathKey] = getter;
  return getter;
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
    let best = {};

    if (keyRanges !== undefined) {
      let { keyPaths, array } = getKeyPaths(store);

      // Primary key is always best.
      if (has.call(keyRanges, keyPaths[0])) {
        return {
          range: keyRanges[keyPaths[0]],
          index: store,
          array,
        }
      }

      for (let i = 0; i < store.indexNames.length; ++i) {
        let n = store.indexNames[i];
        let index = store.index(n);
        let { keyPaths, array } = getKeyPaths(index);
        if (!index.multiEntry && has.call(keyRanges, keyPaths[0])) {
          best = {
            range: keyRanges[keyPaths[0]],
            index,
            array,
          };
        }
      }

      // Prefer unique index
      for (let i = 0; i < store.indexNames.length; ++i) {
        let n = store.indexNames[i];
        let index = store.index(n);
        let { keyPaths, array } = getKeyPaths(index);
        if (!index.multiEntry && index.unique && has.call(keyRanges, keyPaths[0])) {
          best = {
            range: keyRanges[keyPaths[0]],
            index,
            array,
          };
        }
      }
    }

    return best;
  }

  execute(context, keyRanges) {
    let store = context.transaction.objectStore(this.name);
    let best = this.chooseBestIndex(store, keyRanges);

    if (best.range === undefined) {
      return rangeStream(store, null);
    } else {
      let observable = Observable.empty();
      let ranges = best.range.prepare(context);
      for (let i = 0; i < ranges.length; ++i) {
        let range = ranges[0];
        if (best.array) {
          range = range.openUpper();
          range.lower = [range.lower];
          range.upper = [range.upper];
          console.log("toArray", range.tree());
        }
        range = idbRange(range);
      
        observable = observable.concat(rangeStream(best.index, range));
      }

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

  delete(context, tuples) {
    let store = context.transaction.objectStore(this.name);
    let getKeyPath = keyPathGetter(store);

    let requests = [];
    for (let i = 0; i < tuples.length; ++i) {
      let key = getKeyPath(tuples[i]);
      let request = store.delete(key);

      // This error handler will be replaced when something subscribes to the observable.
      request.onerror = function(event) {
        throw event.target.error;
      }
      
      requests.push(request);
    };

    if (requests.length === 0)
      return Observable.from([]);

    // In this path, the onsuccess callback provides the value of the auto-generated key.
    return Observable.create(observer => {
      requests.forEach(request => {
        request.onerror = function(event) {
          observer.error(event.target.error);
        };
      });

      requests[requests.length - 1].onsuccess = function(event) {
        for (let i = 0; i < tuples.length; ++i) {
          observer.next(tuples[i]);
          observer.complete();
        }
      }
    });
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
