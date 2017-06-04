"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");
const { Observable } = require("../rx");

const indexedDB = require("fake-indexeddb");
const IDBKeyRange = require("fake-indexeddb/lib/FDBKeyRange");

const { Context, IDBTable, IDBTransaction, Range, select, insert, update } = require("..");

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

      let store = db.createObjectStore("store", {keyPath: "id", autoIncrement: true});
      store.createIndex("byCity", "city", {unique: false});
      store.put({id: 1, city: "San Francisco"});
      store.put({id: 2, city: "San Francisco"});
      store.put({id: 3, city: "New York City"});

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
      book = new IDBTable(db, "book");
      inventoryItem = new IDBTable(db, "inventoryItem");
      store = new IDBTable(db, "store");
    });
  })

  afterEach(function() {
    sandbox.restore();
    db = undefined;
   })

  describe("IDBTable", function() {
    beforeEach(function() {
      context.transaction = db.transaction(["book", "inventoryItem", "store"], "readwrite");
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

      let objectStore = context.transaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore);
      expect(best.range).to.equal(keyRanges.isbn);
    });

    it("chooses primary key as index over unique secondary key", function() {
      let keyRanges = {
        title: new Range("Quarry Memories", "Quarry Memories"),
        isbn: new Range(123456, 123456),
      };

      let objectStore = context.transaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore);
      expect(best.range).to.equal(keyRanges.isbn);
    });

    it("chooses unique secondary key as index over non-unique secondary key", function() {
      let keyRanges = {
        author: new Range("Fred", "Fred"),
        title: new Range("Quarry Memories", "Quarry Memories"),
      };

      let objectStore = context.transaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore.index("byTitle"));
      expect(best.range).to.equal(keyRanges.title);
    });

    it("chooses secondary key as index", function() {
      let keyRanges = {
        author: new Range("Fred", "Fred"),
      };

      let objectStore = context.transaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.equal(objectStore.index("byAuthor"));
      expect(best.range).to.equal(keyRanges.author);
    });

    it("chooses no key if none relevant", function() {
      let keyRanges = {
        rating: new Range(3, 5),
      };

      let objectStore = context.transaction.objectStore("book");
      let best = book.chooseBestIndex(objectStore, keyRanges);
      expect(best.index).to.be.undefined;
      expect(best.range).to.be.undefined;
    });

    it("updates existing rows", function() {
      let observable = book.put(context, [
        {title: "Quarry", isbn: 123456},
        {title: "Water", isbn: 234567},
      ]);
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
      ], {overwrite: false});

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
      ]);

      return resultArray(observable).then(results => {
        expect(results).to.deep.equal([
          {id: 4, city: "Boston"},
        ]);

        let observable = store.execute(context);
        return resultArray(observable).then(results => {
          expect(results).to.deep.equal([
            {id: 1, city: "San Francisco"},
            {id: 2, city: "San Francisco"},
            {id: 3, city: "New York City"},
            {id: 4, city: "Boston"},
          ]);
        });
      });
    })

    it("insert fails for existing row", function() {
      let observable = book.put(context, [
        {title: "Quarry", isbn: 123456}
      ], {overwrite: false});

      return resultArray(observable).then(() => {
        throw new Error("No error");
      }).catch(error => {
        expect(error).to.match(/Constraint/);
      });
    })
  })

  it("wraps transaction with IDBTransaction", function() {
    let transaction = new IDBTransaction(book, db, new Set(["book"], "readonly"));
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

    return resultArray(query()).then(result => {
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

    return resultArray(query()).then(result => {
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
                   .on `store.id == inventoryItem.storeId`
                .where `book.author == 'Fred'`
              .orderBy `book.title`;

    return resultArray(query()).then(result => {
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
                .where `book.author == 'Fred' && inventoryItem.isbn == book.isbn && store.id == inventoryItem.storeId`
              .orderBy `book.title`;

    return resultArray(query()).then(result => {
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

  it("can insert into object store", function() {
    let query = insert `value`
                 .into (book)
                 .from ({value: [
                   { title: "Database", author: "O'Neil", isbn: 9781558603929 },
                 ]})

    return resultArray(query()).then(result => {
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
    let query = update `{ title: old.title.toLowerCase() }`
                 .into (book)

    return resultArray(query()).then(result => {
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
})
