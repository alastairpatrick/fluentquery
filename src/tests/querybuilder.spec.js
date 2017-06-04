"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");

const { ArrayTable, NamedRelation } = require("..");
const { insert, select, update, upsert } = require("../querybuilder");

let sandbox = sinon.sandbox.create();

describe("fluentquery query builder", function() {
  let thing, type;
  let thingStore, typeStore;
  let thingRelation, typeRelation;
  let context;

  beforeEach(function() {
    thing = [
      {id: 1, name: "Apple", calories: 95, type_id: 1},
      {id: 2, name: "Banana", calories: 105, type_id: 1},
      {id: 3, name: "Cake", calories: 235, type_id: 2},
    ];
    thingStore = new ArrayTable(thing);
    thingRelation = new NamedRelation(thingStore, "thing");

    type = [
      {id: 1, name: "Vegetable"},
      {id: 2, name: "Mineral"},
    ];
    typeStore = new ArrayTable(type);
    typeRelation = new NamedRelation(typeStore, "type");
  })

  afterEach(function() {
    sandbox.restore();
  })

  it("builds select-from array", function() {
    let query = select `{name: thing.name}`
                .from ({thing});

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: "thing",
    });

    expect(query.relation().selector.expandedTree()).to.deep.equal({
      class: "Expression",
      source: "{ name: thing.name }",
      dependencies: ["thing"],
    });
  })

  it("supports string interpolation", function() {
    let n = "name";
    let query = select `{name: thing[${n}]}`
                .from ({thing});

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: '{ name: thing[$subs[0]] }',
      relation: "thing",
    });

    expect(query.relation().selector.expandedTree()).to.deep.equal({
      class: "Expression",
      source: '{ name: thing[$subs[0]] }',
      dependencies: ["thing"],
    });
  })

  it("builds select-from two arrays as inner join", function() {
    let query = select `{name: thing.name, type: type.name}`
                 .from ({thing, type});

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name, type: type.name }",
      relation: {
        class: "Join",
        lRelation: "thing",
        rRelation: "type",
      },
    });

    expect(query.relation().selector.expandedTree()).to.deep.equal({
      class: "Expression",
      source: "{ name: thing.name, type: type.name }",
      dependencies: ["thing", "type"],
    });
  })

  it("builds select-from three arrays as inner join", function() {
    let query = select `{name: thing.name}`
                .from ({thing, type, type2: type});

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        lRelation: {
          class: "Join",
          lRelation: "thing",
          rRelation: "type",
        },
        rRelation: "type2",
      },
    });
  })

  it("builds union", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
                .union (
                select `{name: type.name}`
                 .from ({type})
                )

    expect(query.tree()).to.deep.equal({
      class: "SetOperation",
      type: "union",
      lRelation: {
        class: "Select",
        selector: "{ name: thing.name }",
        relation: "thing",
      },
      rRelation: {
        class: "Select",
        selector: "{ name: type.name }",
        relation: "type",
      },
    });
  })

  it("builds unionAll", function() {
    let query = select `{name: a.name}`
                 .from ({
                   a: ( select `{name: thing.name}`
                         .from ({thing})
                    ).unionAll (
                        select `{name: type.name}`
                         .from ({type})
                             )
                 })

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: a.name }",
      relation: {
        class: "NamedRelation",
        name: "a",
        relation: {
          class: "SetOperation",
          type: "unionAll",
          lRelation: {
            class: "Select",
            selector: "{ name: thing.name }",
            relation: "thing",
          },
          rRelation: {
            class: "Select",
            selector: "{ name: type.name }",
            relation: "type",
          },
        },
      },
    });
  })

  it("builds explicit inner join", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
                 .join ({type})
                   .on `thing.type_id === type.id`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        lRelation: "thing",
        rRelation: "type",
        termGroups: [{
          dependencies: ["thing", "type"],
          expression: "thing.type_id === type.id"
        }]
      },
    });
  })

  it("builds left outer join", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
             .leftJoin ({type})
                   .on `thing.type_id === type.id`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        type: "outer",
        lRelation: "thing",
        rRelation: "type",
        termGroups: [{
          dependencies: ["thing", "type"],
          expression: "thing.type_id === type.id"
        }]
      },
    });
  })

  it("builds left anti join", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
             .antiJoin ({type})
                   .on `thing.type_id === type.id`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        type: "anti",
        lRelation: "thing",
        rRelation: "type",
        termGroups: [{
          dependencies: ["thing", "type"],
          expression: "thing.type_id === type.id"
        }]
      },
    });
  })

  it("builds right outer join", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
            .rightJoin ({type})
                   .on `thing.type_id === type.id`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        type: "outer",
        lRelation: "type",
        rRelation: "thing",
        termGroups: [{
          dependencies: ["thing", "type"],
          expression: "thing.type_id === type.id"
        }]
      },
    });
  })

  it("builds full outer join", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
             .fullJoin ({type})
                   .on `thing.type_id === type.id`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "CompositeUnion",
        lRelation: {
          class: "Join",
          type: "outer",
          lRelation: "thing",
          rRelation: "type",
          termGroups: [{
            dependencies: ["thing", "type"],
            expression: "thing.type_id === type.id"
          }]
        },
        rRelation: {
          class: "Join",
          type: "anti",
          lRelation: "type",
          rRelation: "thing",
          termGroups: [{
            dependencies: ["thing", "type"],
            expression: "thing.type_id === type.id"
          }]
        }
      },
    });
  })

  it("splits join predicate terms", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
                 .join ({type})
                   .on `thing.type_id === type.id && thing.id !== 7`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        lRelation: "thing",
        rRelation: "type",
        termGroups: [{
          dependencies: ["thing"],
          expression: "thing.id !== 7",
        }, {
          dependencies: ["thing", "type"],
          expression: "thing.type_id === type.id",
        }],
      },
    });
  })

  it("builds where", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
                .where `thing.id === $p.id1 || thing.id === $p.id2`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Where",
        relation: "thing",
        termGroups: [{
          dependencies: ["thing"],
          expression: "thing.id === $p.id1 || thing.id === $p.id2",
        }],
      },
    });
  })

  it("splits where terms", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
                .where `thing.id !== $p.id1 && thing.id !== $p.id2`;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Where",
        relation: "thing",
        termGroups: [{
          dependencies: ["thing"],
          expression: "thing.id !== $p.id1 && thing.id !== $p.id2",
        }],
      },
    });
  })

  it("builds group by", function() {
    let query = select `{ type_id: thing.type_id,
                          totalCalories: sum(thing.calories),
                        }`
                 .from ({thing})
              .groupBy `{type_id: thing.type_id}`;

    expect(query.tree()).to.deep.equal({
      class: "GroupBy",
      selector: "$g[0] = sum($g[0], thing.calories), { type_id: thing.type_id, totalCalories: $g[0].value }",
      grouper: "{ type_id: thing.type_id }",
      relation: "thing",
    });
  })

  it("builds ascending order by", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
              .orderBy `thing.name` .asc;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "OrderBy",
        ordering: [{
          expression: "thing.name",
          order: 1,
          nulls: 1,
        }],
        relation: "thing",
      },
    });
  })

  it("builds descending order by", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
              .orderBy `thing.name` .desc;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "OrderBy",
        ordering: [{
          expression: "thing.name",
          order: -1,
          nulls: 1,
        }],
        relation: "thing",
      },
    });
  })

  it("builds ascending order by, nulls first", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
              .orderBy `thing.name` .asc.nullsFirst;

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "OrderBy",
        ordering: [{
          expression: "thing.name",
          order: 1,
          nulls: -1,
        }],
        relation: "thing",
      },
    });
  })

  it("builds descending order by, nulls first", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
              .orderBy `thing.name` .desc.nullsFirst

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "OrderBy",
        ordering: [{
          expression: "thing.name",
          order: -1,
          nulls: -1,
        }],
        relation: "thing",
      },
    });
  })

  it("builds subquery", function() {
    let subquery = select `{type: type.name, id: type.id}`
                    .from ({type});

    let query = select `{name: thing.name}`
                 .from ({ thing,
                          type: subquery,
                       });

    expect(query.tree()).to.deep.equal({
      class: "Select",
      selector: "{ name: thing.name }",
      relation: {
        class: "Join",
        lRelation: "thing",
        rRelation: {
          class: "NamedRelation",
          name: "type",
          relation: {
            class: "Select",
            selector: "{ type: type.name, id: type.id }",
            relation: "type",
          },
        },
      },
    });
  })

  it("builds insert", function() {
    let query = insert `value`
                 .into (thingStore)
                 .from ({value: [
                   { name: "Bob" },
                 ]})

    expect(query.tree()).to.deep.equal({
      class: "Put",
      relation: {
        class: "Select",
        selector: "value",
        relation: "value",
      },
      table: {
        class: "ArrayTable",
      },
      options: {
        overwrite: false,
      },
    });
  })

  it("builds upsert", function() {
    let query = upsert `{id: thing.id, name: thing.name.toLowerCase()}`
                 .into (thingStore)
                 .from ({thing})

    expect(query.tree()).to.deep.equal({
      class: "Put",
      relation: {
        class: "Select",
        selector: "{ id: thing.id, name: thing.name.toLowerCase() }",
        relation: "thing",
      },
      table: {
        class: "ArrayTable",
      },
      options: {
        overwrite: true,
      },
    });
  })

  it("builds update", function() {
    let query = update `{name: old.name.toLowerCase()}`
                 .into (thingStore)

    expect(query.tree()).to.deep.equal({
      class: "Put",
      relation: {
        class: "Select",
        selector: "Object.assign({}, old, { name: old.name.toLowerCase() })",
        relation: "old",
      },
      table: {
        class: "ArrayTable",
      },
      options: {
        overwrite: true,
      },
    });
  })

  it("builds explicit memoize", function() {
    let query = select `{name: thing.name}`
                 .from ({thing})
              .memoize;

    expect(query.tree()).to.deep.equal({
      class: "Memoize",
      relation: {
        class: "Select",
        selector: "{ name: thing.name }",
        relation: "thing",
      },
    });
  })

  it("throws on attempt to modify optimized query", function() {
    let query = select `{name: thing.name}`
                 .from ({thing});

    query.finalize();

    expect(function() {
      query = query.orderBy `thing.name`;
    }).to.throw(/finaliz/);
  })

  it("errors early when selector references unknown relation", function() {
    expect(function() {
     let query = select `{name: foo.name}`
                  .from ({thing});
     query.tree();
    }).to.throw(/foo/);
  })

  it("executes join built with query builder", function() {
    let query = select `{name: thing.name}`
                  .from ({thing})
              .orderBy `thing.name` .desc;

    return query.then(result => {
      expect(result).to.deep.equal([
        { name: "Cake" },
        { name: "Banana" },
        { name: "Apple" },
      ]);
    });
  })

  it("runs sub-queries", function() {
    let query = select `{name: thing.name, type_name: type.name}`
                 .from ({ thing: select `thing` .from ({thing}),
                          type: select `type` .from ({type}),
                       })
                .where `thing.type_id === type.id`;

    return query.then(result => {
      expect(result).to.deep.equal([
        { name: "Apple", type_name: "Vegetable" },
        { name: "Banana", type_name: "Vegetable" },
        { name: "Cake", type_name: "Mineral" },
      ]);
    });
  })

  it("runs cached sub-queries only once per execution context", function() {
    let count = 0;
    let tableGetter = () => {
      ++count;
      return type;
    }

    let subquery = select `type`
                    .from ({type: tableGetter})
                 .memoize;

    let query = select `{id1: type1.id, id2: type2.id}`
                 .from ({ type1: subquery,
                          type2: subquery,
                       })
                .where `type1.id === type2.id`;

    return query.then(result => {
      expect(result).to.deep.equal([
        { id1: 1, id2: 1 },
        { id1: 2, id2: 2 },
      ]);

      expect(count).to.equal(1);
    });
  })
  
  it("forEach executes join built with query builder", function() {
    let result = [];

    return select `{name: thing.name}`
          .from ({thing})
          .orderBy `thing.name` .desc
          .forEach (tuple => {
             result.push(tuple);
          }).then(() => {
           expect(result).to.deep.equal([
              { name: "Cake" },
              { name: "Banana" },
              { name: "Apple" },
            ]);
          });
  })
})
