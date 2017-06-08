const { Observable } = require("./rx");

const { PrimaryKey } = require("./expression");
const { ObjectStore } = require("./tree");

const has = Object.prototype.hasOwnProperty;


class JSONObjectStore extends ObjectStore {
  constructor(object) {
    super();
    this.object = object;
  }

  execute(context) {
    return Observable.create(observer => {
      for (let n in this.object) {
        if (has.call(this.object, n))
          observer.next(Object.assign({[PrimaryKey]: n}, this.object[n]));
      }
      observer.complete();
    });
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
