"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const {
  Transaction,
} = require("..");

let sandbox = sinon.sandbox.create();

describe("transaction", function() {
  let transaction;
  let object;

  beforeEach(function() {
    transaction = new Transaction();
    object = {
      existing: 1,
    };
  })

  it("one view per object per transaction", function() {
    let view1 = transaction.view(object);
    let view2 = transaction.view(object);
    expect(view1).to.equal(view2);

    let transaction2 = new Transaction();
    let view3 = transaction2.view(object);
    expect(view1).to.not.equal(view3);
  })

  it("can modify view of object without modifying underlying", function() {
    let view = transaction.view(object);
    view.existing = 2;
    expect(object.existing).to.equal(1);
  })

  it("onComplete applies property changes to underlying", function() {
    let view = transaction.view(object);
    view.existing = 2;
    transaction.onComplete();
    expect(object.existing).to.equal(2);
  })

  it("onComplete applies property deletions to underlying", function() {
    let view = transaction.view(object);
    view.existing = undefined;
    transaction.onComplete();
    expect(object).to.not.have.property("existing");
  })

  it("onComplete applies property changes to underlying only once", function() {
    let view = transaction.view(object);
    view.existing = 2;
    transaction.onComplete();
    expect(object.existing).to.equal(2);
    object.existing = 3;
    transaction.onComplete();
    expect(object.existing).to.equal(3);
  })

  it("onComplete does nothing after onAbort", function() {
    let view = transaction.view(object);
    view.existing = 2;
    transaction.onAbort();
    transaction.onComplete();
    expect(object.existing).to.equal(1);
  })
})
