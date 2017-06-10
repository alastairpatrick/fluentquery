const { Observable } = require("./rx");

const { PrimaryKey } = require("./expression");
const { ObjectStore } = require("./tree");

const has = Object.prototype.hasOwnProperty;


class JSONObjectStore extends ObjectStore {
  constructor(tuples) {
    super();
    this.tuples = tuples;
  }

  execute(context, keyRanges) {
    let view = context.transaction.view(this.tuples);

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

  put(context, tuples, overwrite) {
    let view = context.transaction.view(this.tuples);

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
    let view = context.transaction.view(this.tuples);

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
}
