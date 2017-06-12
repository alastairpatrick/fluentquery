"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");
const { Observable } = require("../rx");
const sortBy = require("lodash/sortBy");

const {
  Aggregate,
  JSONObjectStore,
  Context,
  Expression,
  GroupBy,
  Join,
  OrderBy,
  PrimaryKey,
  Relation,
  Select,
  SetOperation,
  NamedRelation,
  TermGroups,
  Transaction,
  Where,
  Write,
  parseExpression,
  select,
  traverse,
} = require("..");

let sandbox = sinon.sandbox.create();

const resultArray = (observable) => {
  return new Promise((resolve, reject) => {
    observable.toArray().subscribe(resolve, reject);
  });
}

describe("Tree", function() {
  let thing, type;
  let thingStore, typeStore;
  let thingRelation, typeRelation;
  let context;
  let visitor;

  beforeEach(function() {
    thing = [
      {id: 1, name: "Apple", calories: 95, type_id: 1},
      {id: 2, name: "Banana", calories: 105, type_id: 1},
      {id: 3, name: "Cake", calories: 235, type_id: 2},
    ];
    thingStore = new JSONObjectStore(thing);
    thingRelation = new NamedRelation(thingStore, "thing");

    type = [
      {id: 1, name: "Vegetable"},
      {id: 2, name: "Mineral"},
    ];
    typeStore = new JSONObjectStore(type);
    typeRelation = new NamedRelation(typeStore, "type");
    context = new Context({ p1: 1, p2: 2 });
    context.transaction = new Transaction();
    visitor = {};
  })

  afterEach(function() {
    sandbox.restore();
  })

  describe("Select", function() {
    it("accepts", function() {
      visitor.Select = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      let select = new Select(thingRelation,
                              new Expression(({thing}) => ({}), {thing}));
      traverse(select, visitor);
      sinon.assert.calledOnce(visitor.Select);
      sinon.assert.calledOnce(visitor.NamedRelation);
    })

    it("selects columns from a relation", function() {
      let select = new Select(thingRelation,
                              new Expression(({thing}) => ({ name: thing.name, energy: thing.calories * 4184 }), {thing}));
      return resultArray(select.execute(context)).then(result => {
        expect(result).to.deep.equal([
          { energy: 397480, name: "Apple" },
          { energy: 439320, name: "Banana" },
          { energy: 983240, name: "Cake" },
        ]);
      });
    })

    it("selects parameter", function() {
      let select = new Select(thingRelation,
                              new Expression(function({thing}) { return { p1: this.params.p1, p2: this.params.p2 }}, {thing}));
      return resultArray(select.execute(context)).then(result => {
        expect(result).to.deep.equal([
          { p1: 1, p2: 2 },
          { p1: 1, p2: 2 },
          { p1: 1, p2: 2 },
        ]);
      });
    })
  })

  describe("Write", function() {
    it("accepts", function() {
      visitor.Write = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      let write = new Write(thingRelation, thingStore)
      traverse(write, visitor);
      sinon.assert.calledOnce(visitor.Write);
      sinon.assert.calledOnce(visitor.NamedRelation);
    })

    it("replaces table with self", function() {
      let write = new Write(thingRelation, thingStore)
      write.overwrite = true;
      return resultArray(write.execute(context)).then(result => {
        expect(result).to.deep.equal([{ count: 3 }]);
      });
    });

    it("applies function to returned tuples", function() {
      let write = new Write(thingRelation, thingStore);
      write.overwrite = true;
      write.returning = new Expression(({thing}) => ({ name: thing.name.toUpperCase() }), {thing});
      return resultArray(write.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {name: "APPLE"},
          {name: "BANANA"},
          {name: "CAKE"},
        ]);
      });
    });
  })

  describe("NamedRelation", function() {
    it("accepts", function() {
      visitor.JSONObjectStore = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      traverse(typeRelation, visitor);
      sinon.assert.calledOnce(visitor.NamedRelation);
      sinon.assert.calledOnce(visitor.JSONObjectStore);
    });

    it("schema", function() {
      expect(typeRelation.schema()).to.deep.equal({type: typeRelation});
    });

    it("executes", function() {
      return resultArray(typeRelation.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {type: {id: 1, name: "Vegetable"}},
          {type: {id: 2, name: "Mineral"}},
        ]);
      });
    });

    it("filters tuples", function() {
      typeRelation.predicates.push(new Expression(({type}) => type.id === 1, {type}));
      return resultArray(typeRelation.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {type: {id: 1, name: "Vegetable"}},
        ]);
      });
    })
  })

  describe("SetOperation", function() {
    it("accepts", function() {
      visitor.SetOperation = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      let setOperation = new SetOperation(typeRelation, thingRelation, "union");
      traverse(setOperation, visitor);
      sinon.assert.calledOnce(visitor.SetOperation);
      sinon.assert.calledTwice(visitor.NamedRelation);
    });

    it("executes union", function() {
      let setOperation = new SetOperation(typeRelation, thingRelation, "union");
      return resultArray(setOperation.execute(context)).then(result => {
        result = sortBy(result, r => (r.thing || r.type).name);
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2},
          },
          {
            type: {id: 2, name: "Mineral"}
          },
          {
            type: {id: 1, name: "Vegetable"}
          },
        ]);
      });
    });

    it("union eliminates duplicates", function() {
      let setOperation = new SetOperation(typeRelation, typeRelation, "union");
      return resultArray(setOperation.execute(context)).then(result => {
        result = sortBy(result, r => r.type.name);
        expect(result).to.deep.equal([
          {
            type: {id: 2, name: "Mineral"}
          },
          {
            type: {id: 1, name: "Vegetable"}
          },
        ]);
      });
    });

    it("unionAll keeps duplicates", function() {
      let setOperation = new SetOperation(typeRelation, typeRelation, "unionAll");
      return resultArray(setOperation.execute(context)).then(result => {
        result = sortBy(result, r => r.type.name);
        expect(result).to.deep.equal([
          {
            type: {id: 2, name: "Mineral"}
          },
          {
            type: {id: 2, name: "Mineral"}
          },
          {
            type: {id: 1, name: "Vegetable"}
          },
          {
            type: {id: 1, name: "Vegetable"}
          },
        ]);
      });
    });
  })

  describe("Join", function() {
    it("accepts", function() {
      visitor.Join = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      let join = new Join(thingRelation, typeRelation);
      traverse(join, visitor);
      sinon.assert.calledOnce(visitor.Join);
      sinon.assert.calledTwice(visitor.NamedRelation);
    });

    it("schema of join is union of left and right schemas", function() {
      let join = new Join(thingRelation, typeRelation);
      expect(join.schema()).to.deep.equal({thing: thingRelation, type: typeRelation});
    });

    it("executes cross join if no predicates", function() {
      let join = new Join(thingRelation, typeRelation);
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
            type: {id: 2, name: "Mineral"}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
            type: {id: 2, name: "Mineral"}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2},
            type: {id: 2, name: "Mineral"}
          },
        ]);
      });
    });

    it("executes inner join using predicate", function() {
      thing.push({id: 4, name: "Pie", type_id: 3});

      let join = new Join(thingRelation, typeRelation);
      typeRelation.predicates.push(new Expression(({thing, type}) => thing.type_id == type.id, {thing: thingRelation, type: typeRelation}));
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2},
            type: {id: 2, name: "Mineral"}
          },
        ]);
      });
    });

    it("executes outer join using predicate", function() {
      thing.push({id: 4, name: "Pie", calories: 300, type_id: 3});

      let join = new Join(thingRelation, typeRelation, "outer");
      typeRelation.predicates.push(new Expression(({thing, type}) => thing.type_id == type.id, {thing: thingRelation, type: typeRelation}));
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2},
            type: {id: 2, name: "Mineral"}
          },
          {
            thing: {id: 4, name: "Pie", calories: 300, type_id: 3},
            type: {$otherwise: true}
          },
        ]);
      });
    });

    it("executes anti join using predicate", function() {
      thing.push({id: 4, name: "Pie", calories: 300, type_id: 3});

      let join = new Join(thingRelation, typeRelation, "anti");
      typeRelation.predicates.push(new Expression(({thing, type}) => thing.type_id == type.id, {thing: thingRelation, type: typeRelation}));
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 4, name: "Pie", calories: 300, type_id: 3},
            type: {$otherwise: true}
          },
        ]);
      });
    });

    it("predicate can use parameters", function() {
      thing.push({id: 4, name: "Pie", type_id: 3});

      let join = new Join(thingRelation, typeRelation);
      typeRelation.predicates.push(new Expression(function({thing, type}) { return thing.type_id == type.id && this.params.p1 < this.params.p2 }, {thing: thingRelation, type: typeRelation}));
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2},
            type: {id: 2, name: "Mineral"}
          },
        ]);
      });
    });

    it("filters tuples with where predicates", function() {
      let join = new Join(thingRelation, typeRelation);
      typeRelation.predicates.push(new Expression(({thing, type}) => thing.type_id == type.id, {thing: thingRelation, type: typeRelation}));
      join.predicates.push(new Expression(({type}) => type.name === "Vegetable", {type}));
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1},
            type: {id: 1, name: "Vegetable"}
          },
        ]);
      });
    })

    it("filters generated with where predicate", function() {
      thing.push({id: 4, name: "Pie", calories: 300, type_id: 3});

      let join = new Join(thingRelation, typeRelation, "outer");
      typeRelation.predicates.push(new Expression(({thing, type}) => thing.type_id == type.id, {thing: thingRelation, type: typeRelation}));
      join.predicates.push(new Expression(({type}) => type.id === undefined, {type}));
      return resultArray(join.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 4, name: "Pie", calories: 300, type_id: 3},
            type: {$otherwise: true}
          },
        ]);
      });
    });

    it("throws if schemas overlap", function() {
      expect(function() {
        new Join(thingRelation, thingRelation);
      }).to.throw(/thing/);
    });

    it("executes join built with query builder", function() {
      let query = select `{name: thing.name, type_name: type.name}`
                .from({thing})
                .join({type})
                .on `thing.type_id === type.id`;

      return query.then(result => {
        expect(result).to.deep.equal([
          { name: "Apple", type_name: "Vegetable" },
          { name: "Banana", type_name: "Vegetable" },
          { name: "Cake", type_name: "Mineral" },
        ]);
      });
    })
  })

  describe("Where", function() {
    it("accepts", function() {
      visitor.Where = sandbox.stub();
      let termGroups = new TermGroups();
      let where = new Where(thingRelation, termGroups);
      traverse(where, visitor);
      sinon.assert.calledOnce(visitor.Where);
    });

    it("schema", function() {
      let termGroups = new TermGroups()
      let where = new Where(thingRelation, termGroups);
      expect(where.schema()).to.deep.equal({thing: thingRelation});
    });

    it("filters tuples", function() {
      let termGroups = new TermGroups();
      let where = new Where(thingRelation, termGroups);
      let expression = parseExpression("thing.id == 1", {thing: thingRelation});
      where.predicates.push(expression);
      return resultArray(where.execute(context)).then(result => {
        expect(result).to.deep.equal([{
          thing: {id: 1, name: "Apple", calories: 95, type_id: 1},
        }]);
      });
    })
  })

  describe("GroupBy", function() {
    const sum = (state, v) => {
      if (state === undefined) {
        state = {
          value: 0,
        };
      }

      if (v === null || v === undefined)
        return state;

      state.value += v;
      return state;
    }

    it("accepts", function() {
      visitor.GroupBy = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      let groupBy = new GroupBy(thingRelation,
        new Expression(({thing}, $g) => ({ type_id: thing.type_id, totalCalories: ($g[0] = sum($g[0], thing.calories)).value }), {thing}),
        new Expression(({thing}) => ({ type_id: thing.type_id }), {thing}));
      traverse(groupBy, visitor);
      sinon.assert.calledOnce(visitor.GroupBy);
      sinon.assert.calledOnce(visitor.NamedRelation);
    });

    it("schema", function() {
      let groupBy = new GroupBy(thingRelation,
        new Expression(({thing}, $g) => ({ type_id: thing.type_id, totalCalories: ($g[0] = sum($g[0], thing.calories)).value }), {thing}),
        new Expression(({thing}) => ({ type_id: thing.type_id }), {thing}));
      expect(groupBy.schema()).to.be.undefined;
    })

    it("calculates aggregate", function() {
      let groupBy = new GroupBy(thingRelation,
        new Expression(({thing}, $g) => ({ type_id: thing.type_id, totalCalories: ($g[0] = sum($g[0], thing.calories)).value }), {thing}),
        new Expression(({thing}) => ({ type_id: thing.type_id }), {thing}));
      return resultArray(groupBy.execute(context)).then(result => {
        expect(result).to.deep.equal([
          { type_id: 1, totalCalories: 200 },
          { type_id: 2, totalCalories: 235 },
        ]);
      });
    })
  })

  describe("OrderBy", function() {
    it("accepts", function() {
      visitor.OrderBy = sandbox.stub();
      visitor.NamedRelation = sandbox.stub();
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(({thing}) => thing.name, {thing}), order: 1}]);
      traverse(orderBy, visitor);
      sinon.assert.calledOnce(visitor.OrderBy);
      sinon.assert.calledOnce(visitor.NamedRelation);
    });

    it("schema", function() {
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(({thing}) => thing.name), order: 1}]);
      expect(orderBy.schema()).to.deep.equal({ thing: thingRelation});
    })

    it("sorts in ascending order, nulls last", function() {
      thing.push({ id: 4, calories: 0, type_id: 1 });
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(({thing}) => thing.name, {thing}), order: 1, nulls: 1}]);
      return resultArray(orderBy.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2}
          },
          {
            thing: {id: 4, calories: 0, type_id: 1}
          },
        ]);
      });
    })

    it("sorts in ascending order, nulls first", function() {
      thing.push({ id: 4, calories: 0, type_id: 1 });
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(({thing}) => thing.name, {thing}), order: 1, nulls: -1}]);
      return resultArray(orderBy.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 4, calories: 0, type_id: 1}
          },
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2}
          },
        ]);
      });
    })

    it("sorts in descending order, nulls last", function() {
      thing.push({ id: 4, calories: 0, type_id: 1 });
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(({thing}) => thing.name, {thing}), order: -1, nulls: 1}]);
      return resultArray(orderBy.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1}
          },
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1}
          },
          {
            thing: {id: 4, calories: 0, type_id: 1}
          },
        ]);
      });
    })

    it("sorts in descending order, nulls first", function() {
      thing.push({ id: 4, calories: 0, type_id: 1 });
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(({thing}) => thing.name, {thing}), order: -1, nulls: -1}]);
      return resultArray(orderBy.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 4, calories: 0, type_id: 1}
          },
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1}
          },
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1}
          },
        ]);
      });
    })

    it("ordering function can access parameters", function() {
      let orderBy = new OrderBy(thingRelation,
        [{expression: new Expression(function({thing}) { return thing.calories * this.params.factor }, {thing}), order: 1}]);
      context.params = { factor: -1 };
      return resultArray(orderBy.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {
            thing: {id: 3, name: "Cake", calories: 235, type_id: 2}
          },
          {
            thing: {id: 2, name: "Banana", calories: 105, type_id: 1}
          },
          {
            thing: {id: 1, name: "Apple", calories: 95, type_id: 1}
          },
        ]);
      });
    })
  })
})
