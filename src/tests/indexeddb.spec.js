"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");
const { Observable } = require("../rx");

const indexedDB = require("fake-indexeddb");
const IDBKeyRange = require("fake-indexeddb/lib/FDBKeyRange");

const {
  Context,
  TransactionNode,
  PersistentObjectStore,
  PrimaryKey,
  Range,
  deleteFrom,
  getTransaction,
  select,
  insert,
  update
} = require("..");

let sandbox = sinon.sandbox.create();

let databaseIdx = 1;

const createDatabase = () => {
  return new Promise((resolve, reject) => {
    let request = indexedDB.open("test" + databaseIdx, 1);
    ++databaseIdx;

    request.onupgradeneeded = function () {
      let db = request.result;

      let book = db.createObjectStore("book", {keyPath: "isbn"});
      book.createIndex("byAuthor", "author", {unique: false});
      book.createIndex("byTitle", "title", {unique: true});

      book.put({title: "Quarry Memories", author: "Fred", isbn: 123456});
      book.put({title: "Water Buffaloes", author: "Fred", isbn: 234567});
      book.put({title: "Bedrock Nights", author: "Barney", isbn: 345678});

      let store = db.createObjectStore("store", {keyPath: null, autoIncrement: true});
      store.createIndex("byCity", "city", {unique: false});
      store.put({city: "San Francisco"});
      store.put({city: "San Francisco"});
      store.put({city: "New York City"});

      let inventoryItem = db.createObjectStore("inventoryItem", {keyPath: ["storeId", "isbn"]});
      inventoryItem.createIndex("byStoreId", "storeId", {unique: false});
      inventoryItem.createIndex("byISBN", "isbn", {unique: false});
      inventoryItem.put({storeId: 1, isbn: 123456, quantity: 3});
      inventoryItem.put({storeId: 1, isbn: 234567, quantity: 4});
      inventoryItem.put({storeId: 1, isbn: 345678, quantity: 5});
      inventoryItem.put({storeId: 2, isbn: 123456, quantity: 1});
      inventoryItem.put({storeId: 2, isbn: 234567, quantity: 2});
    }

    request.onsuccess = function(event) {
      resolve(event.target.result);
    };

    request.onerror = function(event) {
      throw event.target.error;
    };
  });
}

const resultArray = (observable) => {
  return new Promise((resolve, reject) => {
    observable.toArray().subscribe(resolve, reject);
  });
}

describe("IndexedDB integration", function() {
  let context;
  let book, inventoryItem, store;
  let db;

  beforeEach(function() {
    context = new Context({});  
    return createDatabase().then(db_ => {
      db = db_;
      book = new PersistentObjectStore(db, "book");
      inventoryItem = new PersistentObjectStore(db, "inventoryItem");
      store = new PersistentObjectStore(db, "store");
    });
  })

  afterEach(function() {
    sandbox.restore();
    db = undefined;
   })

  describe("PersistentObjectStore", function() {
    let idbTransaction;

    beforeEach(function() {
      idbTransaction = db.transaction(["book", "inventoryItem", "store"], "readwrite");
      context.transaction = getTransaction(idbTransaction);
    })

    it("retrieves all rows", function() {
      let observable = book.execute(context);
      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {title: "Quarry Memories", author: "Fred", isbn: 123456},
          {title: "Water Buffaloes", author: "Fred", isbn: 234567},
          {title: "Bedrock Nights", author: "Barney", isbn: 345678},
        ]);
      });
    })

    it("retrieves rows matching predicate using index", function() {
      let observable = book.execute(context, {
        author: new Range("Fred", "Fred"),
      });
      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {title: "Quarry Memories", author: "Fred", isbn: 123456},
          {title: "Water Buffaloes", author: "Fred", isbn: 234567},
        ]);
      });
    })

    it("chooses primary key as index", function() {
      let keyRanges = {
        isbn: new Range(123456,123456),
      };

      let objectStore = idbTransaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore);
      expect(best.ranges).to.deep.equal([keyRanges.isbn]);
      expect(best.array).to.be.false;
    });

    it("chooses primary key as index over unique secondary key", function() {
      let keyRanges = {
        title: new Range("Quarry Memories", "Quarry Memories"),
        isbn: new Range(123456, 123456),
      };

      let objectStore = idbTransaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore);
      expect(best.ranges).to.deep.equal([keyRanges.isbn]);
      expect(best.array).to.be.false;
    });

    it("chooses unique secondary key as index over non-unique secondary key", function() {
      let keyRanges = {
        author: new Range("Fred", "Fred"),
        title: new Range("Quarry Memories", "Quarry Memories"),
      };

      let objectStore = idbTransaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore.index("byTitle"));
      expect(best.ranges).to.deep.equal([keyRanges.title]);
      expect(best.array).to.be.false;
    });

    it("chooses secondary key as index", function() {
      let keyRanges = {
        author: new Range("Fred", "Fred"),
      };

      let objectStore = idbTransaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore.index("byAuthor"));
      expect(best.ranges).to.deep.equal([keyRanges.author]);
      expect(best.array).to.be.false;
    });

    it("chooses no key if none relevant", function() {
      let keyRanges = {
        rating: new Range(3, 5),
      };

      let objectStore = idbTransaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.be.undefined;
    });

    it("chooses composite key if first part has equality and last key path has range", function() {
      let keyRanges = {
        storeId: new Range(1, 1),
        isbn: new Range(100, 200),
      };

      let objectStore = idbTransaction.objectStore("inventoryItem");
      let best = inventoryItem.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore);
      expect(best.ranges).to.deep.equal([keyRanges.storeId, keyRanges.isbn]);
      expect(best.array).to.be.true;
    });

    it("uses as much of composite key as possible up to first non-equality range", function() {
      let keyRanges = {
        storeId: new Range(1, 2),
        isbn: new Range(100, 200),
      };

      let objectStore = idbTransaction.objectStore("inventoryItem");
      let best = inventoryItem.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore);
      expect(best.ranges).to.deep.equal([keyRanges.storeId]);
      expect(best.array).to.be.true;
    });

    it("does not use composite key if range unavailable for first part", function() {
      let keyRanges = {
        isbn: new Range(100, 200),
      };

      let objectStore = idbTransaction.objectStore("inventoryItem");
      let best = inventoryItem.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore.index("byISBN"));
      expect(best.ranges).to.deep.equal([keyRanges.isbn]);
    });

    it("updates existing rows", function() {
      let observable = book.put(context, [
        {title: "Quarry", isbn: 123456},
        {title: "Water", isbn: 234567},
      ], true);
      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {title: "Quarry", isbn: 123456},
          {title: "Water", isbn: 234567},
        ]);

        let observable = book.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {title: "Quarry", isbn: 123456},
            {title: "Water", isbn: 234567},
            {title: "Bedrock Nights", author: "Barney", isbn: 345678},
          ]);
        });
      });
    })

    it("inserts new row", function() {
      let observable = book.put(context, [
        {title: "Database", author: "O'Neil", isbn: 9781558603929}
      ], false);

      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {title: "Database", author: "O'Neil", isbn: 9781558603929}
        ]);

        let observable = book.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {title: "Quarry Memories", author: "Fred", isbn: 123456},
            {title: "Water Buffaloes", author: "Fred", isbn: 234567},
            {title: "Bedrock Nights", author: "Barney", isbn: 345678},
            {title: "Database", author: "O'Neil", isbn: 9781558603929},
          ]);
        });
      });
    })

    it("inserts new row with autoincrementing key", function() {
      let observable = store.put(context, [
        {city: "Boston"}
      ], false);

      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {[PrimaryKey]: 4, city: "Boston"},
        ]);

        let observable = store.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {[PrimaryKey]: 1, city: "San Francisco"},
            {[PrimaryKey]: 2, city: "San Francisco"},
            {[PrimaryKey]: 3, city: "New York City"},
            {[PrimaryKey]: 4, city: "Boston"},
          ]);
        });
      });
    })

    it("insert fails for existing row", function() {
      let observable = book.put(context, [
        {title: "Quarry", isbn: 123456}
      ], false);

      return resultArray(observable).then(() => {
        throw new Error("No error");
      }).catch(error => {
        expect(error).to.match(/Constraint/);
      });
    })

    it("updates existing row of store with composite key", function() {
      let observable = inventoryItem.put(context, [
        {storeId: 1, isbn: 234567, quantity: 44},
      ], true);
      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {storeId: 1, isbn: 234567, quantity: 44},
        ]);

        let observable = inventoryItem.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {storeId: 1, isbn: 123456, quantity: 3},
            {storeId: 1, isbn: 234567, quantity: 44},
            {storeId: 1, isbn: 345678, quantity: 5},
            {storeId: 2, isbn: 123456, quantity: 1},
            {storeId: 2, isbn: 234567, quantity: 2},           
          ]);
        });
      });
    })

    it("deletes rows", function() {
      let observable = store.delete(context, [
        {[PrimaryKey]: 3}
      ]);

      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {[PrimaryKey]: 3},
        ]);

        let observable = store.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {[PrimaryKey]: 1, city: "San Francisco"},
            {[PrimaryKey]: 2, city: "San Francisco"},
          ]);
        });
      });
    })

    it("deletes row of store with composite key", function() {
      let observable = inventoryItem.delete(context, [
        {storeId: 1, isbn: 234567, quantity: 4},
      ]);

      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {storeId: 1, isbn: 234567, quantity: 4},
        ]);

        let observable = inventoryItem.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {storeId: 1, isbn: 123456, quantity: 3},
            {storeId: 1, isbn: 345678, quantity: 5},
            {storeId: 2, isbn: 123456, quantity: 1},
            {storeId: 2, isbn: 234567, quantity: 2},      
          ]);
        });
      });
    })

    it("ignores deletion of non-existent key", function() {
      let observable = store.delete(context, [
        {[PrimaryKey]: 333}
      ]);

      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {[PrimaryKey]: 333},
        ]);

        let observable = store.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {[PrimaryKey]: 1, city: "San Francisco"},
            {[PrimaryKey]: 2, city: "San Francisco"},
            {[PrimaryKey]: 3, city: "New York City"},
          ]);
        });
      });
    })
  })

  it("wraps transaction with TransactionNode", function() {
    let transaction = new TransactionNode(book, db, new Set(["book"], "readonly"));
    let observable = transaction.execute(context);
    return resultArray(observable).then(results => {
      expect(results).to.deep.equal([
        {title: "Quarry Memories", author: "Fred", isbn: 123456},
        {title: "Water Buffaloes", author: "Fred", isbn: 234567},
        {title: "Bedrock Nights", author: "Barney", isbn: 345678},
      ]);
    });
  })

  it("can build query and execute against object store", function() {
    let query = select `{title: book.title}`
                 .from ({book})
              .orderBy `book.title`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {title: "Bedrock Nights"},
        {title: "Quarry Memories"},
        {title: "Water Buffaloes"},
      ]);
    });
  })

  it("can build query and execute against object store using its index", function() {
    let query = select `{title: book.title}`
                 .from ({book})
                .where `book.author == 'Fred'`
              .orderBy `book.title`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {title: "Quarry Memories"},
        {title: "Water Buffaloes"},
      ]);
    });
  })

  it("can build join query and execute against object store using its index", function() {
    let query = select `{title: book.title, city: store.city, quantity: inventoryItem.quantity}`
                 .from ({book})
                 .join ({inventoryItem})
                   .on `inventoryItem.isbn == book.isbn`
                 .join ({store})
                   .on `store[PrimaryKey] == inventoryItem.storeId`
                .where `book.author == 'Fred'`
              .orderBy `book.title`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          city: "San Francisco",
          quantity: 3,
          title: "Quarry Memories",
        },
        {
          city: "San Francisco",
          quantity: 1,
          title: "Quarry Memories",
        },
        {
          city: "San Francisco",
          quantity: 4,
          title: "Water Buffaloes",
        },
        {
          city: "San Francisco",
          quantity: 2,
          title: "Water Buffaloes",
        }
      ]);
    });
  })

  it("can build join query and execute against object store using its index 2", function() {
    let query = select `{title: book.title, city: store.city, quantity: inventoryItem.quantity}`
                 .from ({book, inventoryItem, store})
                .where `book.author == 'Fred' && inventoryItem.isbn == book.isbn && store[PrimaryKey] == inventoryItem.storeId`
              .orderBy `book.title`;

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          city: "San Francisco",
          quantity: 3,
          title: "Quarry Memories",
        },
        {
          city: "San Francisco",
          quantity: 1,
          title: "Quarry Memories",
        },
        {
          city: "San Francisco",
          quantity: 4,
          title: "Water Buffaloes",
        },
        {
          city: "San Francisco",
          quantity: 2,
          title: "Water Buffaloes",
        }
      ]);
    });
  })

  it("can query using part of composite primary key", function() {
    let query = select `inventoryItem`
                 .from ({inventoryItem})
                .where `inventoryItem.storeId == 2`

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          storeId: 2,
          isbn: 123456,
          quantity: 1,
        },
        {
          storeId: 2,
          isbn: 234567,
          quantity: 2,
        },
      ]);
    });
  })

  it("can query using whole composite key", function() {
    let query = select `inventoryItem`
                 .from ({inventoryItem})
                .where `inventoryItem.storeId == 1 && inventoryItem.isbn > 200000`

    return query.then(result => {
      expect(result).to.deep.equal([
        {
          storeId: 1,
          isbn: 234567,
          quantity: 4
        },
        {
          storeId: 1,
          isbn: 345678,
          quantity: 5
        }
      ]);
    });
  })

  it("can insert into object store", function() {
    let query = insert `value`
                 .into (book)
                 .from ({value: [
                   { title: "Database", author: "O'Neil", isbn: 9781558603929 },
                 ]})

    return query.then(result => {
      expect(result).to.deep.equal([
        { title: "Database", author: "O'Neil", isbn: 9781558603929 },
      ]);

      return select `book` .from ({book}) .then(results => {
        expect(results).to.deep.equal([
          {title: "Quarry Memories", author: "Fred", isbn: 123456},
          {title: "Water Buffaloes", author: "Fred", isbn: 234567},
          {title: "Bedrock Nights", author: "Barney", isbn: 345678},
          {title: "Database", author: "O'Neil", isbn: 9781558603929},
        ]);
      });
    });
  })

  it("can update tuples in object store", function() {
    let query = update `{ title: this.title.toLowerCase() }`
                 .into (book)

    return query.then(result => {
      expect(result).to.deep.equal([
        {title: "quarry memories", author: "Fred", isbn: 123456},
        {title: "water buffaloes", author: "Fred", isbn: 234567},
        {title: "bedrock nights", author: "Barney", isbn: 345678},
      ]);

      return select `book` .from ({book}) .then(results => {
        expect(results).to.deep.equal([
          {title: "quarry memories", author: "Fred", isbn: 123456},
          {title: "water buffaloes", author: "Fred", isbn: 234567},
          {title: "bedrock nights", author: "Barney", isbn: 345678},
        ]);
      });
    });
  })

  it("can update tuples in object store with null primary key", function() {
    let query = update `{ city: "Boston" }`
                 .into (store)
                .where `this.city == "San Francisco"`

    return query.then(result => {
      expect(result).to.deep.equal([
        {city: "Boston"},
        {city: "Boston"},
      ]);

      return select `store` .from ({store}) .then(results => {
        expect(results).to.deep.equal([
          {city: "Boston"},
          {city: "Boston"},
          {city: "New York City"},
        ]);
      });
    });
  })

  it("can delete tuples from object store", function() {
    let query = deleteFrom (book)
                    .where `this.isbn == 234567`

    return query.then(result => {
      expect(result).to.deep.equal([
        {title: "Water Buffaloes", author: "Fred", isbn: 234567},
      ]);

      return select `book` .from ({book}) .then(results => {
        expect(results).to.deep.equal([
          {title: "Quarry Memories", author: "Fred", isbn: 123456},
          {title: "Bedrock Nights", author: "Barney", isbn: 345678},
        ]);
      });
    });
  })

  it("can execute queries in particular IDB transaction", function() {
    let updateTitle = update `{ title: $newTitle }`
                       .into (book)
                      .where `this.isbn == $isbn`

    let findBuffaloes = select `book`
                         .from ({book})
                        .where `book.title == "Water Buffaloes"`

    let idbTransaction = db.transaction(["book"], "readwrite");
    return findBuffaloes({}, idbTransaction).then(books => {
      return updateTitle({isbn: books[0].isbn, newTitle: "Replaced"}, idbTransaction);
    }).then(updated => {
      let query = select `{title: book.title, isbn: book.isbn}`
                  .from ({book})
                .orderBy `book.title`;

      return query.then(result => {
        expect(result).to.deep.equal([
          {isbn: 345678, title: "Bedrock Nights"},
          {isbn: 123456, title: "Quarry Memories"},
          {isbn: 234567, title: "Replaced"},
        ]);
      });
    });
  })

  it("can execute queries in particular Transaction", function() {
    let updateTitle = update `{ title: $newTitle }`
                       .into (book)
                      .where `this.isbn == $isbn`

    let findBuffaloes = select `book`
                         .from ({book})
                        .where `book.title == "Water Buffaloes"`

    let idbTransaction = db.transaction(["book"], "readwrite");
    let transaction = getTransaction(idbTransaction);
    return findBuffaloes({}, transaction).then(books => {
      return updateTitle({isbn: books[0].isbn, newTitle: "Replaced"}, transaction);
    }).then(updated => {
      let query = select `{title: book.title, isbn: book.isbn}`
                  .from ({book})
                .orderBy `book.title`;

      return query.then(result => {
        expect(result).to.deep.equal([
          {isbn: 345678, title: "Bedrock Nights"},
          {isbn: 123456, title: "Quarry Memories"},
          {isbn: 234567, title: "Replaced"},
        ]);
      });
    });
  })

  it("updates even if observable is not consumed", function() {
    let updateTitle = update `{ title: this.title.toLowerCase() }`
                       .into (book)

    let idbTransaction = db.transaction(["book"], "readwrite");
    let transaction = getTransaction(idbTransaction);
    updateTitle({}, transaction);
    return transaction.then(() => {
      let query = select `{title: book.title, isbn: book.isbn}`
                  .from ({book})
                .orderBy `book.title`;

      return query.then(result => {
        expect(result).to.deep.equal([
          {isbn: 345678, title: "bedrock nights"},
          {isbn: 123456, title: "quarry memories"},
          {isbn: 234567, title: "water buffaloes"},
        ]);
      });
    });
  })
})
