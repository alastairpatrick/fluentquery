"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");

const {
  select,
} = require("..");

let sandbox = sinon.sandbox.create();

describe("Aggregate", function() {
  let table;

  beforeEach(function() {
    table = [];
    for (let i = 0; i < 100; ++i) {
      table.push({
        quartile: (i / 25) | 0,
        i: i === 0 ? undefined : i,
      });
    }
  })

  afterEach(function() {
    sandbox.restore();
  })

  it("calculates avg()", function() {
    let query = select `{ quartile: table.quartile
                        , average: avg(table.i)
                        }`
                 .from ({table})
              .groupBy `table.quartile`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          "quartile": 0,
          "average": 12.5,
        },
        {
          "quartile": 1,
          "average": 37,
        },
        {
          "quartile": 2,
          "average": 62,
        },
        {
          "quartile": 3,
          "average": 87,
        },
      ]);
    });
  })

  it("calculates count()", function() {
    let query = select `{ quartile: table.quartile
                        , num: count(table.i)
                        }`
                 .from ({table})
              .groupBy `table.quartile`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          "quartile": 0,
          "num": 24,
        },
        {
          "quartile": 1,
          "num": 25,
        },
        {
          "quartile": 2,
          "num": 25,
        },
        {
          "quartile": 3,
          "num": 25,
        },
      ]);
    });
  })

  it("calculates max()", function() {
    let query = select `{ quartile: table.quartile
                        , max: max(table.i)
                        }`
                 .from ({table})
              .groupBy `table.quartile`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          "quartile": 0,
          "max": 24,
        },
        {
          "quartile": 1,
          "max": 49,
        },
        {
          "quartile": 2,
          "max": 74,
        },
        {
          "quartile": 3,
          "max": 99,
        },
      ]);
    });
  })

  it("calculates min()", function() {
    let query = select `{ quartile: table.quartile
                        , min: min(table.i)
                        }`
                 .from ({table})
              .groupBy `table.quartile`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          "quartile": 0,
          "min": 1,
        },
        {
          "quartile": 1,
          "min": 25,
        },
        {
          "quartile": 2,
          "min": 50,
        },
        {
          "quartile": 3,
          "min": 75,
        }
      ]);
    });
  })

  it("calculates sum()", function() {
    let query = select `{ quartile: table.quartile
                        , total: sum(table.i)
                        }`
                 .from ({table})
              .groupBy `table.quartile`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          "quartile": 0,
          "total": 300,
        },
        {
          "quartile": 1,
          "total": 925,
        },
        {
          "quartile": 2,
          "total": 1550,
        },
        {
          "quartile": 3,
          "total": 2175,
        },
      ]);
    });
  })
})
