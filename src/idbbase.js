(function() {
  if (this.indexedDB && typeof this.indexedDB === "object") {
    module.exports.indexedDB = this.indexedDB;
    module.exports.cmp = this.indexedDB.cmp.bind(this.indexedDB);
    module.exports.IDBKeyRange = this.IDBKeyRange;
  }
})();
