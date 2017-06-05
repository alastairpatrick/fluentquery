"use strict";

const { TermGroups } = require("./expression");
const { IDBTransaction } = require("./indexeddb");
const { traverse } = require("./traverse");
const { Join, NamedRelation, OrderBy, Relation, Table, Where } = require("./tree");

const has = Object.prototype.hasOwnProperty;

const getAvailableSchema = (path) => {
  let availableSchema = path.node.schema();
  while (path.parentPath) {
    let parentPath = path.parentPath;
    if (parentPath.node instanceof Join && parentPath.node.rRelation == path.node) {
      availableSchema = Object.assign({}, parentPath.node.lRelation.schema(), availableSchema);
    }
    path = parentPath;
  }
  return availableSchema;
}

const hoistPredicates = (root) => {
  let available = new TermGroups();

  let result = traverse(root, {
    Where: {
      enter(path) {
        let { node } = path;
        if (node.schema() !== undefined)
          available.merge(node.termGroups);
        else
          node.predicates = node.predicates.concat(Array.from(node.termGroups.terms.values()).map(t => t.expression()));
      },
      exit(path) {
        if (path.node.schema() !== undefined)
          path.replaceWith(path.node.relation);
      }
    },

    NamedRelation: {
      exit(path) {
        let { node } = path;

        let availableSchema = getAvailableSchema(path);
        for (let [dependencies, term] of available.terms.entries()) {
          let satisfied = true;
          for (let n in dependencies) {
            if (has.call(dependencies, n))
              satisfied = satisfied && (dependencies[n].isSameDependency(availableSchema[n]));
          }

          if (satisfied) {
            node.predicates.push(term.expression());

            let keyRanges = term.keyRanges();
            if (keyRanges && has.call(keyRanges, node.name))
              Object.assign(node.keyRanges, keyRanges[node.name]);

            available.terms.delete(dependencies);
          }
        };
      }
    },

    Join: {
      enter(path) {
        let { node } = path;
        
        let lSchema = node.lRelation.schema();
        let rSchema = node.rRelation.schema();

        if (node.type !== "inner") {
          for (let [dependencies, term] of available.terms.entries()) {
            if (term.keyRanges() !== undefined)
              continue;

            let rDepends = false;
            for (let n in dependencies) {
              if (has.call(dependencies, n))
                rDepends = rDepends || dependencies[n].isSameDependency(rSchema[n]);
            }

            if (rDepends) {
              node.predicates.push(term.expression());
              available.terms.delete(dependencies);
            }
          };
        }

        available.merge(path.node.termGroups);
      }
    },

    OrderBy(path) {
      let { node } = path;
      if (node.relation instanceof OrderBy) {
        node.ordering = node.relation.ordering.concat(node.ordering);
        node.relation = node.relation.relation;
      }
    },
  });

  if (available.terms.size !== 0)
    throw new Error("Some terms were not assigned to nodes");

  return result;
}

const prepareTransaction = (root) => {
  let db;
  let tableNames = new Set();
  let mode = "readonly";

  root = traverse(root, {
    Write(path) {
      mode = "readwrite";
    },

    IDBTable(path) {
      if (path.node.db !== db) {
        if (db) 
          throw new Error("Query accesses more than one database.");
        db = path.node.db;
      }

      tableNames.add(path.node.name);
    }
  });

  if (db !== undefined)
    root = new IDBTransaction(root, db, tableNames, mode);

  return root;
}

const finalize = (root) => {
  root = hoistPredicates(root);
  root = prepareTransaction(root);
  return root;
}

module.exports = {
  finalize,
  hoistPredicates,
  prepareTransaction,
}
