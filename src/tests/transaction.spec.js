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
  
  beforeEach(function() {
    transaction = new Transaction();
    completed = sinon.stub();
    transaction.on("complete", completed);
    aborted = sinon.stub();
    transaction.on("abort", aborted);
  })

  it("not initially settled", function() {
    expect(transaction.settled).to.be.false;
  })
  
  it("complete settles", function() {
    transaction.complete();
    expect(transaction.settled).to.be.true;      
    return transaction.then(() => {
    }).catch(error => {
      expect.fail("Caught error");
    });
  })
  
  it("abort settles", function() {
    transaction.abort();
    expect(transaction.settled).to.be.true;      
    return transaction.then(() => {
      expect.fail("Transaction should not succeed");
    }).catch(error => {
    });
  })

  it("complete emits event", function() {
    transaction.complete();
    sinon.assert.calledOnce(completed);
    sinon.assert.notCalled(aborted);
    return transaction.then(() => {
    }).catch(error => {
      expect.fail("Caught error");
    });
  })

  it("abort emits event", function() {
    transaction.abort();
    sinon.assert.calledOnce(aborted);
    sinon.assert.notCalled(completed);
    return transaction.then(() => {
      expect.fail("Transaction should not succeed");
    }).catch(error => {
    });
  })

  it("can only complete once", function() {
    transaction.complete();
    transaction.complete();
    sinon.assert.calledOnce(completed);
    sinon.assert.notCalled(aborted);
    return transaction.then(() => {
    }).catch(error => {
      expect.fail("Caught error");
    });
  })

  it("cannot complete after aborting", function() {
    transaction.abort();
    transaction.complete();
    sinon.assert.calledOnce(aborted);
    sinon.assert.notCalled(completed);
    return transaction.then(() => {
      expect.fail("Transaction should not succeed");
    }).catch(error => {
    });
  })

  it("cannot abort once", function() {
    transaction.abort();
    transaction.abort();
    sinon.assert.calledOnce(aborted);
    sinon.assert.notCalled(completed);
    return transaction.then(() => {
      expect.fail("Transaction should not succeed");
    }).catch(error => {
    });
  })

  it("cannot abort after completing", function() {
    transaction.complete();
    transaction.abort();
    sinon.assert.calledOnce(completed);
    sinon.assert.notCalled(aborted);
    return transaction.then(() => {
    }).catch(error => {
      expect.fail("Caught error");
    });
  })
  
  it("automatically settles", function(done) {
    transaction.delayComplete();
    setImmediate(() => {
      expect(transaction.settled).to.be.false;
      setImmediate(() => {
        expect(transaction.settled).to.be.true;
        done();
      });
    });
  })
  
  it("can delay automatic settles", function(done) {
    transaction.delayComplete();
    setImmediate(() => {
      expect(transaction.settled).to.be.false;
      transaction.delayComplete();
      setImmediate(() => {
        expect(transaction.settled).to.be.false;
        setImmediate(() => {
          expect(transaction.settled).to.be.true;
          done();
        });
      });
    });
  })
  
  it("automatically completes after delayComplete()", function(done) {
    transaction.delayComplete();
    transaction.on("complete", () => {
      done();
    });
  })
  
  it("automatically resolves after delayComplete()", function(done) {
    transaction.delayComplete();
    transaction.then(() => {
      done();
    });
  })
  
  it("automatically completes even if delayComplete() never called", function(done) {
    transaction.on("complete", () => {
      done();
    });
  })
  
  it("automatically resolves even if delayComplete() never called", function(done) {
    transaction.then(() => {
      done();
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

  it("delays transaction completion", function(done) {
    let tuples = {
      a: {title: "A"},
    };
    let objectStore = new JSONObjectStore(tuples);
    let transactionNode = new TransactionNode(objectStore);
    setImmediate(() => {
      expect(transaction.settled).to.be.false;
      context.execute(transactionNode);
      setImmediate(() => {
        expect(transaction.settled).to.be.false;
        setImmediate(() => {
          expect(transaction.settled).to.be.true;
          done();
        });
      });
    });
  })
})
