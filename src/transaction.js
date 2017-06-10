"use strict";

const has = Object.prototype.hasOwnProperty;

class Transaction {
  // Do not invoke Transaction with an IDBTransaction directly. Call getTransaction instread.
  constructor() {
    this.viewMap = new Map();
    this.onAbort = this.onAbort.bind(this);
    this.onComplete = this.onComplete.bind(this);
  }

  view(object) {
    let view = this.viewMap.get(object);
    if (view !== undefined)
      return view;
    
    view = Object.create(object);
    this.viewMap.set(object, view);
    return view;
  }

  onAbort() {
    this.viewMap = undefined;
  }

  onComplete() {
    if (this.viewMap === undefined)
      return;

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
  }
}

const transactions = new WeakMap();

const getTransaction = (idbTransaction) => {
  let transaction = transactions.get(idbTransaction);
  if (transaction !== undefined)
    return transaction;
  
  transaction = new Transaction();
  transaction.idbTransaction = idbTransaction;
  idbTransaction.addEventListener("abort", transaction.onAbort);
  idbTransaction.addEventListener("complete", transaction.onComplete);

  transactions.set(idbTransaction, transaction);
  return transaction;
}

module.exports = {
  Transaction,
  getTransaction,
}
