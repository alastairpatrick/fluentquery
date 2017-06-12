"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const {
  Context,
  JSONObjectStore,
  NamedRelation,
  PrimaryKey,
  Select,
  Transaction,
  TransactionNode,
  Write,
  parseExpression,
} = require("..");

let sandbox = sinon.sandbox.create();

describe("Transaction", function() {
  let transaction;
  let completed;
  let aborted;
  let context;
  let object;
  
  beforeEach(function() {
    transaction = new Transaction();
    completed = sinon.stub();
    transaction.on("complete", completed);
    aborted = sinon.stub();
    transaction.on("abort", aborted);

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

  it("complete applies property changes to underlying", function() {
    let view = transaction.view(object);
    view.existing = 2;
    transaction.complete();
    expect(object.existing).to.equal(2);
  })

  it("complete applies property deletions to underlying", function() {
    let view = transaction.view(object);
    view.existing = undefined;
    transaction.complete();
    expect(object).to.not.have.property("existing");
  })

  it("complete applies property changes to underlying only once", function() {
    let view = transaction.view(object);
    view.existing = 2;
    transaction.complete();
    expect(object.existing).to.equal(2);
    object.existing = 3;
    transaction.complete();
    expect(object.existing).to.equal(3);
  })

  it("complete does nothing after abort", function() {
    let view = transaction.view(object);
    view.existing = 2;
    transaction.abort();
    return transaction.then(() => {}).catch(error => {
      transaction.complete();
      expect(object.existing).to.equal(1);
    });
  })

  it("not initially settled", function() {
    expect(transaction.settled).to.be.false;
  })
  
  it("complete settles", function() {
    transaction.complete();
    expect(transaction.settled).to.be.true;      
  })
  
  it("abort settles", function() {
    transaction.abort();
    expect(transaction.settled).to.be.true;      
  })

  it("complete emits event", function() {
    transaction.complete();
    sinon.assert.calledOnce(completed);
    sinon.assert.notCalled(aborted);
  })

  it("abort emits event", function() {
    transaction.abort();
    sinon.assert.calledOnce(aborted);
    sinon.assert.notCalled(completed);
  })

  it("can only complete once", function() {
    transaction.complete();
    transaction.complete();
    sinon.assert.calledOnce(completed);
    sinon.assert.notCalled(aborted);
  })

  it("cannot complete after aborting", function() {
    transaction.abort();
    transaction.complete();
    sinon.assert.calledOnce(aborted);
    sinon.assert.notCalled(completed);
  })

  it("cannot abort once", function() {
    transaction.abort();
    transaction.abort();
    sinon.assert.calledOnce(aborted);
    sinon.assert.notCalled(completed);
  })

  it("cannot abort after completing", function() {
    transaction.complete();
    transaction.abort();
    sinon.assert.calledOnce(completed);
    sinon.assert.notCalled(aborted);
  })

  it("complete resolves transaction", function() {
    transaction.complete();
    return transaction.then(() => {
    }).catch(error => {
      expect.fail("Caught error");
    });
  })

  it("abort rejects transaction", function() {
    transaction.abort(new Error("Foo"));
    return transaction.then(() => {
      expect.fail("Transaction shoould not succeed");
    }).catch(error => {
      expect(error).to.match(/Foo/);
    });
  })

  it("abort aborts IDB transaction", function() {
    transaction.idbTransaction = {
      abort: sinon.stub(),
    };
    transaction.abort(new Error("Foo"))
    return transaction.then(() => {}, error => {
      sinon.assert.calledOnce(transaction.idbTransaction.abort);
    });
  })
})

describe("TransactionNode", function() {
  let transaction, context;

  beforeEach(function() {
    transaction = new Transaction();
    context = new Context();
    context.transaction = transaction;
  })

  it("aborts transaction on Write error", function() {
    let tuples = {
      a: {title: "A"},
    };
    let objectStore = new JSONObjectStore(tuples);

    // Will attempt to insert existing rows, causing a failing primary key conflict.
    let write = new Write(objectStore, objectStore);

    let transactionNode = new TransactionNode(write);
    
    context.execute(transactionNode).subscribe((v) => {
      expect.fail("Unexpected next tuple");
    }, error => {
      expect(error).to.match(/'a'/);
    }, complete => {
      expect.fail("Not expected to complete");
    });

    return transaction.then(() => {
      expect.fail("Transaction should not succeed.");
    }).catch(error => {
      expect(transaction.settled).to.be.true;
      expect(error).to.match(/'a'/);
    });
  })

  it("aborts transaction on expression exception", function() {
    let tuples = {
      a: {title: "A"},
    };
    let objectStore = new JSONObjectStore(tuples);
    let named = new NamedRelation(objectStore, "o");
    let select = new Select(named, parseExpression("{ title: (() => { throw new Error('foo') })() }", {o: named}, []));
    let transactionNode = new TransactionNode(select);
    
    context.execute(transactionNode).subscribe((v) => {
      expect.fail("Unexpected next tuple");
    }, error => {
      expect(error).to.match(/foo/);
    }, complete => {
      expect.fail("Not expected to complete");
    });

    return transaction.then(() => {
      expect.fail("Transaction should not succeed.");
    }).catch(error => {
      expect(transaction.settled).to.be.true;
      expect(error).to.match(/foo/);
    });
  })

  it("skips query if transaction already completed", function() {
    let tuples = {
      a: {title: "A"},
    };
    let objectStore = new JSONObjectStore(tuples);
    let transactionNode = new TransactionNode(objectStore);
    transaction.complete();
    context.execute(transactionNode).subscribe((v) => {
      expect.fail("Unexpected next tuple");
    }, error => {
      expect(error).to.match(/settled/);
    }, complete => {
      expect.fail("Not expected to complete");
    });
  })

  it("skips query if transaction already aborted", function() {
    let tuples = {
      a: {title: "A"},
    };
    let objectStore = new JSONObjectStore(tuples);
    let transactionNode = new TransactionNode(objectStore);
    transaction.abort();
    transaction.then(() => {}).catch(error => {});
    context.execute(transactionNode).subscribe((v) => {
      expect.fail("Unexpected next tuple");
    }, error => {
      expect(error).to.match(/settled/);
    }, complete => {
      expect.fail("Not expected to complete");
    });
  })
})
