const { Observable } = require("./rx");

const { PrimaryKey } = require("./expression");
const { ObjectStore } = require("./tree");

const has = Object.prototype.hasOwnProperty;

const transactionHelpers = new WeakMap();

class TransactionHelper {
  constructor(transaction) {
    this.viewMap = new Map();
    transaction.on("complete", this.onComplete, this);
    transaction.on("abort", this.onAbort, this);
  }

  getView(object) {
    let view = this.viewMap.get(object);
    if (view !== undefined)
      return view;
    
    view = Object.create(object);
    this.viewMap.set(object, view);
    return view;
  }
  
  onComplete() {
    for (let [object, view] of this.viewMap) {
      for (let n in view) {
        if (has.call(view, n)) {
          let v = view[n];
          if (v === undefined)
            delete object[n];
          else
            object[n] = view[n];
        }
      }
    }

    this.viewMap = undefined;
  }

  onAbort() {
    this.viewMap = undefined;
  }
}

const getJSONView = (transaction, object) => {
  let helper = transactionHelpers.get(transaction);
  if (helper === undefined) {
    helper = new TransactionHelper(transaction);
    transactionHelpers.set(transaction, helper);
  }

  return helper.getView(object);
}

class JSONObjectStore extends ObjectStore {
  constructor(tuples) {
    super();
    this.tuples = tuples;
  }

  execute(context, keyRanges) {
    let view = getJSONView(context.transaction, this.tuples);

    if (keyRanges && has.call(keyRanges, PrimaryKey)) {
      let keyRange = keyRanges[PrimaryKey];
      if (keyRange.isEquality()) {
        let prepared = keyRanges[PrimaryKey].prepare(context);
        return Observable.create(observer => {
          for (let i = 0; i < prepared.length; ++i) {
            let n = prepared[i].lower;
            let v = view[n];
            if (v !== undefined)
              observer.next(Object.assign({[PrimaryKey]: n}, v));
          }
          observer.complete();
        });
      }
    }

    return Observable.create(observer => {
      for (let n in view) {
        let v = view[n];
        if (v !== undefined)
          observer.next(Object.assign({[PrimaryKey]: n}, v));
      }
      observer.complete();
    });
  }

  put(context, tuples, overwrite, wantGenerated) {
    let view = getJSONView(context.transaction, this.tuples);

    if (!overwrite) {
      for (let i = 0; i < tuples.length; ++i) {
        let tuple = tuples[i];
        let k = tuple[PrimaryKey];
        if (view[k] !== undefined) {
          return Observable.create(observer =>
            observer.error(new Error(`Tuple with primary key '${k}' already exists.`)));
        }
      }
    }

    for (let i = 0; i < tuples.length; ++i) {
      let tuple = tuples[i];
      let k = tuple[PrimaryKey];
      let v = Object.assign({}, tuple);
      delete v[PrimaryKey];
      view[k] = v;
    }

    return Observable.from(tuples);
  }

  delete(context, tuples) {
    let view = getJSONView(context.transaction, this.tuples);

    for (let i = 0; i < tuples.length; ++i) {
      let tuple = tuples[i];
      let k = tuple[PrimaryKey];

      // When modifying the view, assign undefined to identify deleted tuples. When the transaction commits, the underlying tuple will be deleted for real.
      view[k] = undefined;
    }
    return Observable.from(tuples);
  }

  tree() {
    return {
      class: this.constructor.name,
    };;
  }
};

module.exports = {
  JSONObjectStore,
  getJSONView,
}
