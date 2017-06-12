"use strict";

const EventEmitter = require('eventemitter3');
const { Observable } = require("./rx");

const has = Object.prototype.hasOwnProperty;

class Transaction extends EventEmitter {
  constructor() {
    super();
    this.settled = false;
    this.viewMap = new Map();
    
    this.promise = new Promise((resolve, reject) => {
      this.complete = (v) => {
        this.onComplete();
        resolve(v);
        return this;
      };
      this.abort = (error) => {
        reject(error);
        this.onAbort();
        return this;
      };
    });
  }

  view(object) {
    let view = this.viewMap.get(object);
    if (view !== undefined)
      return view;
    
    view = Object.create(object);
    this.viewMap.set(object, view);
    return view;
  }
  
  onComplete() {
    if (this.settled)
      return;
    this.settled = true;

    for (let [object, view] of this.viewMap) {
      for (let n in view) {
        if (has.call(view, n)) {
          let v = view[n];
          if (v === undefined)
            delete object[n];
          else
            object[n] = view[n];
        }
      }
    }

    this.viewMap = undefined;
    this.emit("complete");
  }

  onAbort() {
    if (this.settled)
      return;
    this.settled = true;

    this.viewMap = undefined;
    if (this.idbTransaction)
      this.idbTransaction.abort();

    this.emit("abort");
  }

  then(resolved, rejected) {
    return this.promise.then(resolved, rejected);
  }
}

const transactions = new WeakMap();

const getTransaction = (idbTransaction) => {
  let transaction = transactions.get(idbTransaction);
  if (transaction !== undefined)
    return transaction;
  
  transaction = new Transaction();
  transaction.idbTransaction = idbTransaction;

  idbTransaction.addEventListener("abort", () => {
    transaction.abort(idbTransaction.error);
  });
  idbTransaction.addEventListener("complete", () => {
    transaction.complete();
  });

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
  Transaction,
  TransactionNode,
  getTransaction,
}
