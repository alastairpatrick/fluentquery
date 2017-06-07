const { insert, select, update, upsert, PersistentObjectStore } = require("../src");

const createDatabase = () => {
  return new Promise((resolve, reject) => {
    let request = indexedDB.open("test", 1);

    request.onupgradeneeded = function () {
        let db = request.result;

        let book = db.createObjectStore("book", {keyPath: "isbn"});
        book.createIndex("byAuthor", "author", {unique: false});
        book.createIndex("byTitle", "title", {unique: true});

        book.put({title: "Quarry Memories", author: "Fred", isbn: 123456});
        book.put({title: "Water Buffaloes", author: "Fred", isbn: 234567});
        book.put({title: "Bedrock Nights", author: "Barney", isbn: 345678});

        let store = db.createObjectStore("store", {keyPath: "id"});
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
      reject(event.target.error);
    };
  });
}

createDatabase().then(db_ => {
  // These are in the global scope so you can use/call them interactively from the JavaScript console.
  window.db = db_;
  window.select = select;
  window.insert = insert;
  window.update = update;
  window.upsert = upsert;
  window.book = new PersistentObjectStore(db, "book");
  window.inventoryItem = new PersistentObjectStore(db, "inventoryItem");
  window.store = new PersistentObjectStore(db, "store");
  
  console.log("Database ready");
});
