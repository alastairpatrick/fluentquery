"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");

const { traverse, traversePath } = require("..");

let sandbox = sinon.sandbox.create();

describe("Traverse", function() {
  let foo, visitor;

  class Foo {
    accept(context) {
    }
  }

  beforeEach(function() {
    foo = new Foo();
    visitor = {};
  })

  afterEach(function() {
    sandbox.restore();
  })

  it("traverses object with no accept method", function() {
    class NoAccept {
    };
    traverse(new NoAccept(), visitor);
  })

  it("invokes default enter function for Foo", function() {
    visitor.Foo = sandbox.stub();
    traverse(foo, visitor);
    sinon.assert.calledOnce(visitor.Foo);
  })

  it("invokes explicit enter and exit function Foo", function() {
    visitor.Foo = {
      enter: sandbox.stub(),
      exit: sandbox.stub(),
    };
    traverse(foo, visitor);
    sinon.assert.calledOnce(visitor.Foo.enter);
    sinon.assert.calledOnce(visitor.Foo.exit);
    sinon.assert.callOrder(visitor.Foo.enter, visitor.Foo.exit);
  })

  it("invokes common enter function", function() {
    visitor.enter = sandbox.stub();
    visitor.Foo = {
      enter: sandbox.stub(),
    };
    traverse(foo, visitor);
    sinon.assert.calledOnce(visitor.enter);
    sinon.assert.callOrder(visitor.Foo.enter, visitor.enter);
  })

  it("invokes common exit function", function() {
    visitor.exit = sandbox.stub();
    visitor.Foo = {
      exit: sandbox.stub(),
    };
    traverse(foo, visitor);
    sinon.assert.calledOnce(visitor.exit);
    sinon.assert.callOrder(visitor.exit, visitor.Foo.exit);
  })

  it("can access parent path", function() {
    class B {
    }

    class A {
      constructor() {
        this.b = new B();
      }

      accept(context) {
        traversePath(this, "b", context);
      }
    }

    let a = new A();
    traverse(a, {
      B(path) {
        expect(path.parentPath.node).to.equal(a);
      }
    });
  })

  it("replaces paths", function() {
    class B {
    }

    class A {
      constructor() {
        this.b = new B();
      }

      accept(context) {
        traversePath(this, "b", context);
      }
    }

    let a = new A();
    traverse(a, {
      B(path) {
        path.replaceWith("hello");
      }
    });

    expect(a.b).to.equal("hello");
  })

  it("traverses null", function() {
    traverse(null, visitor);
  })

  it("traverses undefined", function() {
    traverse(null, visitor);
  })
})