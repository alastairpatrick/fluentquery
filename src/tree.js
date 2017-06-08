"use strict";

const { ValueMap, ValueSet } = require("valuecollection");

const { Observable } = require("./rx");
const { Aggregate } = require("./aggregate");
const { TermGroups } = require("./expression");
const { cmp } = require("./idbbase");
const { traversePath } = require("./traverse");

const has = Object.prototype.hasOwnProperty;

const applyPredicates = (observable, predicates, context) => {
  if (predicates.length === 0)
    return observable;

  let predicateFns = predicates.map(p => p.prepare(context));
  let contextTuple = context.tuple;
  return observable.filter(tuple => {
    for (let i = 0; i < predicateFns.length; ++i) {
      if (!predicateFns[i](Object.assign({}, contextTuple, tuple)))
        return false;
    }
    return true;
  });
}

class Context {
  constructor(arg0, arg1) {
    if (arg0 instanceof Context) {
      Object.assign(this, arg0, arg1);
    } else {
      this.params = arg0;
      this.relationMemo = new Map();
      this.tuple = {};
    }
  }

  execute(node) {
    let observable = node.execute(this);
    return observable;
  }
}

class Relation {
  schema() {
    return undefined;
  }

  isSameDependency(that) {
    return this === that;
  }
}

class ObjectStore extends Relation {
  constructor() {
    super();
  }
}

// This represents SQL SELECT rather than relational algebra SELECT. In relational algebra
// terms, this resembles projection, i.e. selecting particular columns.
class Select extends Relation {
  constructor(relation, selector) {
    super();
    this.relation = relation;
    this.selector = selector;
  }

  execute(context) {
    let selectorFn = this.selector.prepare(context);
    return context.execute(this.relation).map(selectorFn);
  }

  accept(context) {
    traversePath(this, "relation", context);
    traversePath(this, "selector", context);
  }

  tree() {
    let result = {
      class: this.constructor.name,
      relation: this.relation.tree(),
      selector: this.selector.tree(),
    }
    
    return result;
  }
}

class Write extends Relation {
  constructor(relation, objectStore, options) {
    super();
    this.relation = relation;
    this.objectStore = objectStore;
    this.options = options;
  }

  execute(context) {
    let observable = context.execute(this.relation);
    
    let method;
    if (this.options.delete) {
      method = this.objectStore.delete.bind(this.objectStore);
    } else {
      method = this.objectStore.put.bind(this.objectStore);
    }

    // All the tuples are collected in an array before applying any modifications
    // so that the modifications are not prematurely visible to the query.
    return observable.toArray().map(tuples => {
      return method(context, tuples, this.options.overwrite);
    }).mergeAll();
  }

  accept(context) {
    traversePath(this, "relation", context);
    traversePath(this, "objectStore", context);
  }

  tree() {
    let result = {
      class: this.constructor.name,
      relation: this.relation.tree(),
      objectStore: this.objectStore.tree(),
      options: this.options,
    }
    
    return result;
  }
}

class NamedRelation extends Relation {
  constructor(relation, name) {
    super();
    this.relation = relation;
    this.name = name;
    this.predicates = [];
    this.keyRanges = {};
  }

  schema() {
    return { [this.name]: this };
  }

  execute(context) {
    let observable = this.relation.execute(context, this.keyRanges).map(tuple => ({ [this.name]: tuple }));
    observable = applyPredicates(observable, this.predicates, context);
    return observable;
  }

  accept(context) {
    traversePath(this, "relation", context);
  }

  tree() {
    if (this.relation instanceof ObjectStore) {
      return this.name;
    } else {
      let result = {
        class: this.constructor.name,
        name: this.name,
        relation: this.relation.tree(),
      };
      if (this.predicates.length > 0) {
        result.predicates = this.predicates.map(p => p.tree());
      }
      return result;
    }
  }
}

class CompositeUnion extends Relation {
  constructor(lRelation, rRelation, type) {
    super();
    this.lRelation = lRelation;
    this.rRelation = rRelation;
  }

  schema() {
    return this.lRelation.schema();
  }

  execute(context) {
    return Observable.merge(context.execute(this.lRelation), context.execute(this.rRelation));
  }

  accept(context) {
    traversePath(this, "lRelation", context);
    traversePath(this, "rRelation", context);
  }

  tree() {
    return {
      class: this.constructor.name,
      lRelation: this.lRelation.tree(),
      rRelation: this.rRelation.tree(),
    }
  }
}

class SetOperation extends Relation {
  constructor(lRelation, rRelation, type) {
    super();
    this.lRelation = lRelation;
    this.rRelation = rRelation;
    this.type = type;
  }

  execute(context) {
    let observable;
    if (this.type === "union" || this.type === "unionAll") {
      observable = context.execute(this.lRelation).merge(context.execute(this.rRelation));
    } else {
      throw new Error(`Unknown set operation '${this.type}'.`);
    }

    if (this.type === "union") {
      let set = new ValueSet();
      observable = observable.filter(tuple => set.add(tuple));
    }

    return observable;
  }

  accept(context) {
    traversePath(this, "lRelation", context);
    traversePath(this, "rRelation", context);
  }

  tree() {
    return {
      class: this.constructor.name,
      lRelation: this.lRelation.tree(),
      rRelation: this.rRelation.tree(),
      type: this.type,
    }
  }
}

class Join extends Relation {
  constructor(lRelation, rRelation, type="inner") {
    super();
    this.lRelation = lRelation;
    this.rRelation = rRelation;
    this.type = type;

    this.termGroups = new TermGroups();
    this.predicates = [];

    let lSchema = this.lRelation.schema();
    let rSchema = this.rRelation.schema();
    for (let n in lSchema) {
      if (has.call(lSchema, n) && has.call(rSchema, n))
        throw new Error(`Cannot join relations that share "${n}".`);
    }
  }

  schema() {
    return Object.assign({}, this.lRelation.schema(), this.rRelation.schema());
  }

  execute(context) {
    let otherwiseTuple;
    if (this.type !== "inner") {
      otherwiseTuple = {};
      let schema = this.rRelation.schema();
      for (let n in schema) {
        if (has.call(schema, n))
          otherwiseTuple[n] = { $otherwise: true };
      }
    }

    let observable = context.execute(this.lRelation);

    observable = observable.mergeMap(aTuple => {
      let rightContext = new Context(context, {
        tuple: Object.assign({}, context.tuple, aTuple),
      });
      let observable = rightContext.execute(this.rRelation).map(bTuple => Object.assign({}, aTuple, bTuple));
     
      if (otherwiseTuple) {
        let generatedTuple = Object.assign({}, aTuple, otherwiseTuple);

        if (this.type === "anti")
          observable = observable.isEmpty().filter(t => t).map(() => generatedTuple);
        else
          observable = observable.defaultIfEmpty(generatedTuple);
      }
      return observable;
    });

    observable = applyPredicates(observable, this.predicates, context);

    return observable;
  }

  accept(context) {
    traversePath(this, "lRelation", context);
    traversePath(this, "rRelation", context);
    this.predicates.forEach((p, i) => traversePath(this.predicates, i, context));
  }

  tree() {
    let result = {
      class: this.constructor.name,
      lRelation: this.lRelation.tree(),
      rRelation: this.rRelation.tree(),
    }

    if (this.type !== "inner")
      result.type = this.type;

    if (this.predicates.length)
      result.predicates = this.predicates.map(p => p.tree());

    let joinTree = this.termGroups.tree();
    if (joinTree.length !== 0)
      result.termGroups = joinTree;
      
    return result;
  }
}

class Where extends Relation {
  constructor(relation, termGroups) {
    super();
    this.relation = relation;
    this.termGroups = termGroups;
    this.predicates = [];
  }

  schema() {
    return this.relation.schema();
  }

  execute(context) {
    let observable = context.execute(this.relation);
    observable = applyPredicates(observable, this.predicates, context);
    return observable;
  }

  accept(context) {
    traversePath(this, "relation", context);
    traversePath(this, "termGroups", context);
  }

  tree() {
    return {
      class: this.constructor.name,
      termGroups: this.termGroups.tree(),
      relation: this.relation.tree(),
    };
  }
}

class GroupBy extends Relation {
  constructor(relation, selector, grouper) {
    super();
    this.relation = relation;
    this.selector = selector;
    this.grouper = grouper;
  }

  execute(context) {
    let params = context.params;

    let grouperFn = this.grouper.prepare(context);
    let selectorFn = this.selector.prepare(context);

    const reduceStep = (map, tuple) => {
      let groupKey = grouperFn(tuple);
      let group = map.get(groupKey);
      let state;
      if (group === undefined) {
        state = [];
        group = { state };
        map.set(groupKey, group);
      } else {
        state = group.state;
      }

      group.tuple = selectorFn(tuple, state);

      return map;
    }

    const extractTotals = (map) => {
      return Array.from(map.values()).map(group => group.tuple);
    }

    let observable = context.execute(this.relation);
    return observable.reduce(reduceStep, new ValueMap())
                     .mergeMap(extractTotals);
  }

  accept(context) {
    traversePath(this, "relation", context);
    traversePath(this, "selector", context);
    traversePath(this, "grouper", context);
  }

  tree() {
    return {
      class: this.constructor.name,
      selector: this.selector.tree(),
      grouper: this.grouper.tree(),
      relation: this.relation.tree(),
    };
  }
}

class OrderBy extends Relation {
  constructor(relation, ordering) {
    super();
    this.relation = relation;
    this.ordering = ordering;
  }

  schema() {
    return this.relation.schema();
  }
  
  execute(context) {
    let observable = context.execute(this.relation);
    let fns = this.ordering.map(o => o.expression.prepare(context));
    let ordering = this.ordering;

    return observable.toArray().map(tuples => {
      tuples.sort((a, b) => {
        for (let i = 0; i < fns.length; ++i) {
          let fn = fns[i];
          let aa = fn(a), bb = fn(b);

          if (aa === undefined || aa === null) {
            if (bb !== undefined && nn !== null)
              return ordering[i].nulls;
          } else {
            if (bb === undefined || bb === null) {
              return -ordering[i].nulls;
            } else {
              let c = cmp(aa, bb);
              if (c !== 0)
                return c * ordering[i].order;
            }
          }
        }
        return 0;
      });
      return Observable.from(tuples);
    }).mergeAll();
  }

  accept(context) {
    traversePath(this, "relation", context);
    traversePath(this, "ordering", context);
  }

  tree() {
    return {
      class: this.constructor.name,
      ordering: this.ordering.map(o => ({
        expression: o.expression.tree(),
        order: o.order,
        nulls: o.nulls,
      })),
      relation: this.relation.tree(),
    };
  }
}

class Memoize extends Relation {
  constructor(relation) {
    super();
    this.relation = relation;
  }

  schema() {
    return this.relation.schema();
  }

  execute(context) {
    let observable = context.relationMemo.get(this);
    if (observable !== undefined)
      return observable;
    
    observable = context.execute(this.relation).publishReplay();
    observable.connect();
    context.relationMemo.set(this, observable);
    return observable;
  }

  accept(context) {
    traversePath(this, "relation", context);
  }

  tree() {
    return {
      class: this.constructor.name,
      relation: this.relation.tree(),
    };
  }
}

module.exports = {
  CompositeUnion,
  Context,
  GroupBy,
  Join,
  Memoize,
  NamedRelation,
  OrderBy,
  Relation,
  Select,
  SetOperation,
  ObjectStore,
  Where,
  Write,
};
