"use strict";

const EventEmitter = require('eventemitter3');
const { Observable } = require("./rx");

const has = Object.prototype.hasOwnProperty;

class BaseTransaction extends EventEmitter {
  constructor() {
    super();
    this.settled = false;

    this.promise = new Promise((resolve, reject) => {
      this.onComplete = (v) => {
        if (!this.settled) {
          this.settled = true;
          this.emit("complete");
          resolve(v);
        }
        return this;
      };
      this.onAbort = (error) => {
        if (!this.settled) {
          this.settled = true;
          this.emit("abort");
          reject(error);
        }
        return this;
      };
    });
  }

  complete() {
    this.onComplete();
  }

  abort(error) {
    this.onAbort(error);
  }

  delayComplete() {
  }

  then(resolved, rejected) {
    return this.promise.then(resolved, rejected);
  }
}

class PersistentTransaction extends BaseTransaction {
  constructor(idbTransaction) {
    super();
    this.idbTransaction = idbTransaction;

    idbTransaction.addEventListener("abort", () => {
      this.onAbort(this.idbTransaction.error);
    });
    idbTransaction.addEventListener("complete", () => {
      this.onComplete();
    });
  }

  abort(error) {
    this.idbTransaction.abort();
    super.abort(error);
  }
}

class Transaction extends BaseTransaction {
  constructor() {
    super();
    this.immediateId = undefined;
    this.delayComplete();
  }

  delayComplete() {
    if (this.immediateId !== undefined)
      clearImmediate(this.immediateId);
    this.immediateId = setImmediate(() => {
      this.immediateId = setImmediate(() => {
        this.immediateId = undefined;
        this.complete();
      });
    });
  }
}

const transactions = new WeakMap();

const getTransaction = (idbTransaction) => {
  let transaction = transactions.get(idbTransaction);
  if (transaction !== undefined)
    return transaction;
  
  transaction = new PersistentTransaction(idbTransaction);

  transactions.set(idbTransaction, transaction);
  return transaction;
}

class TransactionNode {
  constructor(relation, db, objectStoreNames, mode) {
    this.relation = relation;
    this.db = db;
    this.objectStoreNames = objectStoreNames;
    this.mode = mode;
  }

  execute(context) {
    context.db = this.db;
    if (context.transaction === undefined) {
      if (this.db)
        context.transaction = getTransaction(this.db.transaction(Array.from(this.objectStoreNames), this.mode));
      else
        context.transaction = new Transaction();
    }

    if (context.transaction.settled)
      return Observable.throw(new Error("Transaction already settled."));

    context.transaction.delayComplete();

    return context.execute(this.relation).catch(error => {
      context.transaction.abort(error);
      return Observable.throw(error);
    });
  }

  tree() {
    return {
      class: this.constructor.name,
      objectStoreNames: Array.from(this.objectStoreNames).sort(),
      relation: this.relation.tree(),
      mode: this.mode,
    };
  }
}

module.exports = {
  BaseTransaction,
  Transaction,
  TransactionNode,
  getTransaction,
}
