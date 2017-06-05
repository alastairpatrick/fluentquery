"use strict";

const Symbol = require('es6-symbol');

const { TermGroups, parseExpression } = require("./expression");
const { finalize } = require("./finalize");
const { traverse } = require("./traverse");

const {
  ArrayTable,
  CompositeUnion,
  Context,
  FunctionTable,
  GroupBy,
  Join,
  Memoize,
  NamedRelation,
  OrderBy,
  Put,
  Relation,
  Select,
  SetOperation,
  Where,
} = require("./tree");

const QUERY = Symbol("QUERY");

const has = Object.prototype.hasOwnProperty;

const makeInnerJoin = (relationMap) => {
  let relations = [];
  for (let n in relationMap) {
    if (has.call(relationMap, n)) {
      let relation = relationMap[n];

      if (relation instanceof Relation) {
        // fin
      } else if (Array.isArray(relation)) {
        relation = new ArrayTable(relation);
      } else if (typeof relation === "function") {
        if (relation[QUERY])
          relation = relation.relation();
        else
          relation = new FunctionTable(relation);
      } else {
        throw new Error(`Bad relation type for "${n}".`);
      }

      relations.push(new NamedRelation(relation, n));
    }
  }

  let buildRelation = relations[0];
  for (let i = 1; i < relations.length; ++i)
    buildRelation = new Join(buildRelation, relations[i], "inner");
  
  return buildRelation;
}

const makeDefinitions = (values) => {
  let result = {};
  for (let n in values) {
    if (has.call(values, n))
      result[n] = Object.getOwnPropertyDescriptor(values, n);
  }
  result[QUERY] = { value: true };
  return result;
}

const DEFAULT_MODE = {
  joinRelations: undefined,
  orderBy: undefined,
  finalized: false,
}

const newQuery = (command) => {
  let buildRelation = undefined;
  let queryRelation = undefined;
  let mode = Object.assign({}, DEFAULT_MODE);

  let selector = undefined;
  let selectorSubst = undefined;
  let into = undefined;
  let memoize = false;

  const query = (params={}, transaction=undefined) => {
    let relation = query.finalize();

    let context = new Context(params);
    context.transaction = transaction;
    return context.execute(relation);
  }

  const chain = (newRelation, newMode) => {
    if (mode.finalized)
      throw new Error("Cannot modify query after finalization");

    buildRelation = newRelation;
    queryRelation = undefined;

    Object.assign(mode, DEFAULT_MODE, newMode);

    return query;
  }

  Object.defineProperties(query, makeDefinitions({
    relation() {
      if (queryRelation)
        return queryRelation;

      queryRelation = buildRelation;

      if (selector) {
        queryRelation = new Select(buildRelation, parseExpression(selector, buildRelation.schema(), selectorSubst));
        selector = undefined;
      }

      if (command === "select") {
        if (memoize)
          queryRelation = new Memoize(queryRelation);
      } else {
        if (into === undefined)
          throw new Error("Use into() to specify table to be updated.");

        queryRelation = new Put(queryRelation, into, { overwrite: command !== "insert" });
      }

      return queryRelation;
    },

    finalize() {
      if (!mode.finalized) {
        queryRelation = finalize(query.relation());
        mode.finalized = true;
      }
      return queryRelation;
    },
    
    tree() {
      return query.relation().tree()
    },

    select(sel, ...args) {
      if (selector !== undefined)
        throw new Error("select() already called");
      selector = sel;
      selectorSubst = args;

      if (command === "update") {
        selector = Array.from(selector);
        selector[0] = "Object.assign({}, old, " + selector[0];
        selector[selector.length - 1] += ")";
      }

      return chain(buildRelation);
    },

    from(relationMap) {
      return chain(makeInnerJoin(relationMap));
    },

    into(table) {
      if (into !== undefined)
        throw new Error("into() already called");
      into = table;

      if (command === "update")
        buildRelation = makeInnerJoin({old: table});

      return chain(buildRelation);
    },

    union(rQuery) {
      return chain(new SetOperation(query.relation(), rQuery.relation(), "union"));
    },

    unionAll(rQuery) {
      return chain(new SetOperation(query.relation(), rQuery.relation(), "unionAll"));
    },

    join(relationMap) {
      let join = new Join(buildRelation, makeInnerJoin(relationMap), "inner");
      return chain(join, {
        joinRelations: [join],
      });
    },

    antiJoin(relationMap) {
      let join = new Join(buildRelation, makeInnerJoin(relationMap), "anti");
      return chain(join, {
        joinRelations: [join],
      });
    },

    leftJoin(relationMap) {
      let join = new Join(buildRelation, makeInnerJoin(relationMap), "outer");
      return chain(join, {
        joinRelations: [join],
      });
    },

    rightJoin(relationMap) {
      let join = new Join(makeInnerJoin(relationMap), buildRelation, "outer");
      return chain(join, {
        joinRelations: [join],
      });
    },

    fullJoin(relationMap) {
      let outerJoin = new Join(buildRelation, makeInnerJoin(relationMap), "outer");
      let antiJoin = new Join(makeInnerJoin(relationMap), buildRelation, "anti");
      return chain(new CompositeUnion(outerJoin, antiJoin), {
        joinRelations: [outerJoin, antiJoin],
      });
    },

    on(predicate, ...args) {
      let schema = buildRelation.schema();
      mode.joinRelations.forEach(r => {
        r.termGroups.parse(predicate, schema, args);
      });
      return chain(buildRelation, {
        joinRelations: mode.joinRelations,
      });
    },

    where(predicate, ...args) {
      let termGroups = new TermGroups();
      termGroups.parse(predicate, buildRelation.schema(), args);
      return chain(new Where(buildRelation, termGroups));
    },

    groupBy(grouper, ...args) {
      if (selector) {
        // The expression that would have been used for the Select node is used as the selector
        // for the GroupBy node and no Select will be generated.
        let result = chain(new GroupBy(buildRelation,
                                        parseExpression(selector, buildRelation.schema(), selectorSubst, { allowAggregates: true }),
                                        parseExpression(grouper, buildRelation.schema(), args)));
        selector = undefined;
        selectorSubst = undefined;
        return result;
      } else {
        throw new Error("Only one groupBy per query");
      }
    },

    orderBy(ordering, ...args) {
      let expression = parseExpression(ordering, buildRelation.schema(), args);
      let orderBy = new OrderBy(buildRelation, [{expression, order: 1, nulls: 1}]);
      return chain(orderBy, { orderBy });
    },

    get asc() {
      let lastOrdering = mode.orderBy.ordering[mode.orderBy.ordering.length - 1];
      lastOrdering.order = 1;
      return chain(buildRelation, {
        orderBy: mode.orderBy,
      });
    },

    get desc() {
      let lastOrdering = mode.orderBy.ordering[mode.orderBy.ordering.length - 1];
      lastOrdering.order = -1;
      return chain(buildRelation, {
        orderBy: mode.orderBy,
      });
    },

    order(order) {
      let lastOrdering = mode.orderBy.ordering[mode.orderBy.ordering.length - 1];
      lastOrdering.order = order;
      return chain(buildRelation, {
        orderBy: mode.orderBy,
      });
    },

    get nullsFirst() {
      let lastOrdering = mode.orderBy.ordering[mode.orderBy.ordering.length - 1];
      lastOrdering.nulls = -1;
      return chain(buildRelation, {
        orderBy: mode.orderBy,
      });
    },

    get nullsLast() {
      let lastOrdering = mode.orderBy.ordering[mode.orderBy.ordering.length - 1];
      lastOrdering.nulls = 1;
      return chain(buildRelation, {
        orderBy: mode.orderBy,
      });
    },

    nulls(nulls) {
      let lastOrdering = mode.orderBy.ordering[mode.orderBy.ordering.length - 1];
      lastOrdering.nulls = nulls;
      return chain(buildRelation, {
        orderBy: mode.orderBy,
      });
    },

    get memoize() {
      memoize = true;
      return chain(buildRelation);
    },

    forEach(callback, promiseCtr=Promise) {
      let observable = query();
      return observable.forEach(callback, promiseCtr);
    },

    then(resolved, rejected) {
      return new Promise((resolve, reject) => {
        let observable = query();
        observable.toArray().subscribe(resolve, reject);
      }).then(resolved, rejected);
    }
  }));

  return chain();
}

const select = (selector, ...args) => {
  return newQuery("select").select(selector, ...args);
};

const insert = (selector, ...args) => {
  return newQuery("insert").select(selector, ...args);
};

const upsert = (selector, ...args) => {
  return newQuery("upsert").select(selector, ...args);;
};

const update = (selector, ...args) => {
  return newQuery("update").select(selector, ...args);;
};

module.exports = {
  insert,
  select,
  update,
  upsert,
}

