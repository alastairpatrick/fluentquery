"use strict";

const { Observable } = require("./rx");

const { IDBKeyRange } = require("./idbbase");
const { Range, compositeRange, includes } = require("./range");
const { traversePath } = require("./traverse");
const { PrimaryKey } = require("./expression");
const { Transaction, getTransaction } = require("./transaction");
const { ObjectStore } = require("./tree");

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

const usableKeyRanges = (keyRanges, keyPaths) => {
  let ranges = [];
  for (let i = 0; i < keyPaths.length; ++i) {
    let keyPath = keyPaths[i];
    if (!has.call(keyRanges, keyPath)) {
      return ranges;
    }
    let keyRange = keyRanges[keyPath];
    ranges.push(keyRange);
    if (!keyRange.isEquality()) {
      return ranges;
    }
  }
  return ranges;
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

class PersistentObjectStore extends ObjectStore {
  constructor(db, name) {
    super();

    if (typeof db !== "object")
      throw new Error("Expected IDBDatabase");
    if (typeof name !== "string")
      throw new Error("Expected object store name");

    this.db = db;
    this.name = name;
  }

  chooseBestIndex(store, keyRanges) {
    if (keyRanges === undefined)
      return {};

    // Primary key is always best.
    let { keyPaths, array } = getKeyPaths(store);
    let ranges = usableKeyRanges(keyRanges, keyPaths);
    if (ranges.length)
      return { ranges, index: store, array };

    let best = {};

    for (let i = 0; i < store.indexNames.length; ++i) {
      let n = store.indexNames[i];
      let index = store.index(n);
      if (!index.multiEntry) {
        let { keyPaths, array } = getKeyPaths(index);
        let ranges = usableKeyRanges(keyRanges, keyPaths);
        if (ranges.length)
          best = { ranges, index, array };
      }
    }

    // Prefer unique index
    for (let i = 0; i < store.indexNames.length; ++i) {
      let n = store.indexNames[i];
      let index = store.index(n);
      if (!index.multiEntry && index.unique) {
        let { keyPaths, array } = getKeyPaths(index);
        let ranges = usableKeyRanges(keyRanges, keyPaths);
        if (ranges.length)
          best = { ranges, index, array };
      }
    }

    return best;
  }

  execute(context, keyRanges) {
    let store = context.transaction.idbTransaction.objectStore(this.name);
    let best = this.chooseBestIndex(store, keyRanges);

    if (best.ranges === undefined) {
      return rangeStream(store, null);
    } else {
      console.log(`Using key ${best.index.name} with ${best.ranges.length} key paths`);
      let observable = Observable.empty();
      let equals = [];
      for (let j = 0; j < best.ranges.length - 1; ++j) {
        let prepared = best.ranges[j].prepare(context);
        if (prepared.length === 0)
          return observable;
        if (prepared.length > 1 || !prepared[0].isEquality())
          throw new Error("Initial index ranges must all be equalities");
        equals.push(prepared[0].lower);
      }

      let prepared = best.ranges[best.ranges.length - 1].prepare(context);
      for (let i = 0; i < prepared.length; ++i) {
        let range = prepared[i];
        if (best.array) {
          range = compositeRange(equals, range);
          console.log("comp", equals, range);
        }
        observable = observable.concat(rangeStream(best.index, idbRange(range)));
      }

      return observable;
    }
  }

  put(context, tuples, overwrite) {
    let store = context.transaction.idbTransaction.objectStore(this.name);
    let method;
    if (store.keyPath === null) {
      if (overwrite)
        method = (v) => store.put(v, v[PrimaryKey]);
      else
        method = (v) => store.add(v, v[PrimaryKey]);
    } else {
      if (overwrite)
        method = (v) => store.put(v);
      else
        method = (v) => store.add(v);
    }

    if (store.keyPath === null) {
      method 
    }

    let setKeyPath = keyPathSetter(store);

    let requests = [];
    for (let i = 0; i < tuples.length; ++i) {
      let tuple = tuples[i];
      let request = method(tuple, tuple[PrimaryKey]);

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
    let store = context.transaction.idbTransaction.objectStore(this.name);
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

class TransactionNode {
  constructor(relation, db, objectStoreNames, mode) {
    this.relation = relation;
    this.db = db;
    this.objectStoreNames = objectStoreNames;
    this.mode = mode;
  }

  execute(context) {
    context.db = this.db;
    if (context.transaction === undefined) {
      if (this.db)
        context.transaction = getTransaction(this.db.transaction(Array.from(this.objectStoreNames), this.mode));
      else
        context.transaction = new Transaction();
    }
    return context.execute(this.relation);
  }

  tree() {
    return {
      class: this.constructor.name,
      objectStoreNames: Array.from(this.objectStoreNames).sort(),
      relation: this.relation.tree(),
      mode: this.mode,
    };
  }
}

module.exports = {
  PersistentObjectStore,
  TransactionNode,
};
