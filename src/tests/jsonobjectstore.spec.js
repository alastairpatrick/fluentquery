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
  afterEach(function() {
    sandbox.restore();
  })

  it("accepts", function() {
    let visitor = {
      JSONObjectStore: sandbox.stub(),
    };
    traverse(new JSONObjectStore([]), visitor);
    sinon.assert.calledOnce(visitor.JSONObjectStore);
  })

  describe("wrapping array", function() {
    it("executes", function() {
      let objectStore = new JSONObjectStore([
        {title: "A"},
        {title: "B"},
      ]);
      return resultArray(objectStore.execute(context)).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 0, title: "A"},
          {[PrimaryKey]: 1, title: "B"},
        ]);
      });
    })

    it("extracts particular rows using equality key range", function() {
      let objectStore = new JSONObjectStore([
        {title: "A"},
        {title: "B"},
      ]);
      return resultArray(objectStore.execute(context, {
          [PrimaryKey]: new Range(1, 1),
        })).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 1, title: "B"},
        ]);
      });
    })

    it("executes using range but returns all rows", function() {
      let objectStore = new JSONObjectStore([
        {title: "A"},
        {title: "B"},
        {title: "C"},
      ]);
      return resultArray(objectStore.execute(context, {
          [PrimaryKey]: new Range(1, 2),
        })).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 0, title: "A"},
          {[PrimaryKey]: 1, title: "B"},
          {[PrimaryKey]: 2, title: "C"},
        ]);
      });
    })

    it("updates existing row", function() {
      let tuples = [
        {title: "A"},
        {title: "B"},
      ]
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
        {[PrimaryKey]: 1, title: "NewB"},
      ], true)).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 1, title: "NewB"},
        ]);

        expect(tuples).to.deep.equal([
          {title: "A"},
          {title: "NewB"},
        ]);

        expect(tuples[1][PrimaryKey]).to.be.undefined;
      });
    })

    it("attempting to insert new row with existing key fails", function() {
      let tuples = [
        {title: "A"},
        {title: "B"},
      ]
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
          {[PrimaryKey]: 1, title: "C"},
        ], false)).then(() => {
        expect.fail("Did not fail");
      }).catch(error => {
        expect(error).to.match(/'1'/);
      });
    })

    it("inserts new row", function() {
      let tuples = [
        {title: "A"},
        {title: "B"},
      ]
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
        {[PrimaryKey]: 2, title: "C"},
      ], false)).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 2, title: "C"},
        ]);

        expect(tuples).to.deep.equal([
          {title: "A"},
          {title: "B"},
          {title: "C"},
        ]);
      });
    })

    it("inserting new row in array introduces undefined gap that is visible in underlying array but not through query", function() {
      let tuples = [
        {title: "A"},
        {title: "B"},
      ]
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
        {[PrimaryKey]: 3, title: "C"},
      ], false)).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 3, title: "C"},
        ]);

        expect(tuples).to.deep.equal([
          {title: "A"},
          {title: "B"},
          undefined,
          {title: "C"},
        ]);

        return resultArray(objectStore.execute(context)).then(result => {
          expect(result).to.deep.equal([
            {[PrimaryKey]: 0, title: "A"},
            {[PrimaryKey]: 1, title: "B"},
            {[PrimaryKey]: 3, title: "C"},
          ]);
        });
      });
    })

    it("deletes existing row", function() {
      let tuples = [
        {title: "A"},
        {title: "B"},
        {title: "C"},
      ]
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.delete(context, [
        {[PrimaryKey]: 1, title: "Hello"},
      ])).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 1, title: "Hello"},
        ]);

        expect(tuples).to.deep.equal([
          {title: "A"},
          undefined,
          {title: "C"},
        ]);

        return resultArray(objectStore.execute(context)).then(result => {
          expect(result).to.deep.equal([
            {[PrimaryKey]: 0, title: "A"},
            {[PrimaryKey]: 2, title: "C"},
          ]);
        });
      });
    })

    it("ignores deletion of non-existent row", function() {
      let tuples = [
        {title: "A"},
        {title: "B"},
      ]
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.delete(context, [
        {[PrimaryKey]: 2, title: "Hello"},
      ])).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: 2, title: "Hello"},
        ]);

        expect(tuples).to.deep.equal([
          {title: "A"},
          {title: "B"},
        ]);

        return resultArray(objectStore.execute(context)).then(result => {
          expect(result).to.deep.equal([
            {[PrimaryKey]: 0, title: "A"},
            {[PrimaryKey]: 1, title: "B"},
          ]);
        });
      });
    })
  })

  describe("wrapping Object", function() {
    it("executes JSONObjectStore", function() {
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
    })

    it("extracts particular rows from JSONObjectStore using equality key range", function() {
      let objectStore = new JSONObjectStore({
        a: {title: "A"},
        b: {title: "B"},
      });
      return resultArray(objectStore.execute(context, {
          [PrimaryKey]: new Range("b", "b"),
        })).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: "b", title: "B"},
        ]);
      });
    })

    it("executes JSONObjectStore using range but returns all rows", function() {
      let objectStore = new JSONObjectStore({
        a: {title: "A"},
        b: {title: "B"},
        c: {title: "C"},
      });
      return resultArray(objectStore.execute(context, {
          [PrimaryKey]: new Range("b", "c"),
        })).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: "a", title: "A"},
          {[PrimaryKey]: "b", title: "B"},
          {[PrimaryKey]: "c", title: "C"},
        ]);
      })
    })

    it("updates existing row", function() {
      let tuples = {
        a: {title: "A"},
        b: {title: "B"},
      };
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
        {[PrimaryKey]: "b", title: "NewB"},
      ], true)).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: "b", title: "NewB"},
        ]);

        expect(tuples).to.deep.equal({
          a: {title: "A"},
          b: {title: "NewB"},
        });

        expect(tuples.b[PrimaryKey]).to.be.undefined;
      });
    })

    it("attempting to insert new row with existing key fails", function() {
      let tuples = {
        a: {title: "A"},
        b: {title: "B"},
      };
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
          {[PrimaryKey]: "b", title: "C"},
        ], false)).then(() => {
        expect.fail("Did not fail");
      }).catch(error => {
        expect(error).to.match(/'b'/);
      });
    })

    it("inserts new row", function() {
      let tuples = {
        a: {title: "A"},
        b: {title: "B"},
      };
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.put(context, [
        {[PrimaryKey]: "c", title: "C"},
      ], false)).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: "c", title: "C"},
        ]);

        expect(tuples).to.deep.equal({
          a: {title: "A"},
          b: {title: "B"},
          c: {title: "C"},
        });
      });
    })

    it("deletes existing row", function() {
      let tuples = {
        a: {title: "A"},
        b: {title: "B"},
        c: {title: "C"},
      };
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.delete(context, [
        {[PrimaryKey]: "b", title: "Hello"},
      ])).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: "b", title: "Hello"},
        ]);

        expect(tuples).to.deep.equal({
          a: {title: "A"},
          c: {title: "C"},
        });

        return resultArray(objectStore.execute(context)).then(result => {
          expect(result).to.deep.equal([
            {[PrimaryKey]: "a", title: "A"},
            {[PrimaryKey]: "c", title: "C"},
          ]);
        });
      });
    })

    it("ignores deletion of non-existent row", function() {
      let tuples = {
        a: {title: "A"},
        b: {title: "B"},
      };
      let objectStore = new JSONObjectStore(tuples);
      return resultArray(objectStore.delete(context, [
        {[PrimaryKey]: "c", title: "Hello"},
      ])).then(result => {
        expect(result).to.deep.equal([
          {[PrimaryKey]: "c", title: "Hello"},
        ]);

        expect(tuples).to.deep.equal({
          a: {title: "A"},
          b: {title: "B"},
        });

        return resultArray(objectStore.execute(context)).then(result => {
          expect(result).to.deep.equal([
            {[PrimaryKey]: "a", title: "A"},
            {[PrimaryKey]: "c", title: "B"},
          ]);
        });
      });
    })
  })
})
