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
    if (keyRanges && has.call(keyRanges, PrimaryKey)) {
      let keyRange = keyRanges[PrimaryKey];
      if (keyRange.isEquality()) {
        let prepared = keyRanges[PrimaryKey].prepare(context);
        return Observable.create(observer => {
          for (let i = 0; i < prepared.length; ++i) {
            let n = prepared[i].lower;
            observer.next(Object.assign({[PrimaryKey]: n}, this.tuples[n]));
          }
          observer.complete();
        });
      }
    }

    return Observable.create(observer => {
      for (let n in this.tuples) {
        if (has.call(this.tuples, n))
          observer.next(Object.assign({[PrimaryKey]: n}, this.tuples[n]));
      }
      observer.complete();
    });
  }

  put(context, tuples, overwrite) {
    if (!overwrite) {
      for (let i = 0; i < tuples.length; ++i) {
        let tuple = tuples[i];
        let k = tuple[PrimaryKey];
        if (has.call(this.tuples, k)) {
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
      this.tuples[k] = v;
    }

    return Observable.from(tuples);
  }

  delete(context, tuples) {
    for (let i = 0; i < tuples.length; ++i) {
      let tuple = tuples[i];
      let k = tuple[PrimaryKey];
      delete this.tuples[k];
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
