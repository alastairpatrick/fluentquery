"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");

const {
  Context,
  JSONObjectStore,
  NamedRelation,
  PrimaryKey,
  Range,
  traverse,
} = require("..");

let sandbox = sinon.sandbox.create();

const resultArray = (observable) => {
  return new Promise((resolve, reject) => {
    observable.toArray().subscribe(resolve, reject);
  });
}

describe("JSONObjectStore", function() {
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
    visitor = {};
  })

  afterEach(function() {
    sandbox.restore();
  })

  it("accepts", function() {
    visitor.JSONObjectStore = sandbox.stub();
    traverse(typeStore, visitor);
    sinon.assert.calledOnce(visitor.JSONObjectStore);
  })

  it("executes JSONObjectStore wrapping array", function() {
    return resultArray(typeStore.execute(context)).then(result => {
      expect(result).to.deep.equal(type);
    });
  })

  it("executes JSONObjectStore wrapping object", function() {
    let objectStore = new JSONObjectStore({
      a: {title: "A"},
      b: {title: "B"},
    });
    return resultArray(objectStore.execute(context)).then(result => {
      expect(result).to.deep.equal([
        {[PrimaryKey]: "a", title: "A"},
        {[PrimaryKey]: "b", title: "B"},
      ]);
    });
  });
})
