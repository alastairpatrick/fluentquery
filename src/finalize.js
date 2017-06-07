"use strict";

const { TermGroups } = require("./expression");
const { IDBTransaction } = require("./indexeddb");
const { RangeIntersection } = require("./range");
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
        if (node.schema() !== undefined) {
          available.merge(node.termGroups);
        } else {
          if (node.termGroups.terms.length) {
            let mergedTerm = node.termGroups.terms.reduce((merged, term) => merged.merge(term));
            node.predicates.push(mergedTerm.expression());
          }
        }
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

        let terms = [];
        available.terms = available.terms.filter(term => {
          let dependencies = term.dependencies;
          let satisfied = true;
          for (let n in dependencies) {
            if (has.call(dependencies, n))
              satisfied = satisfied && (dependencies[n].isSameDependency(availableSchema[n]));
          }

          if (satisfied)
            terms.push(term);

          return !satisfied;
        });

        if (terms.length) {
          let mergedTerm = terms.reduce((merged, term) => merged.merge(term))
          node.predicates.push(mergedTerm.expression());

          let keyRanges = mergedTerm.keyRanges();
          if (keyRanges && has.call(keyRanges, node.name)) {
            for (let keyPath in keyRanges[node.name]) {
              if (has.call(keyRanges[node.name], keyPath)) {
                if (has.call(node.keyRanges, keyPath))
                  node.keyRanges[keyPath] = new RangeIntersection(node.keyRanges[keyPath], keyRanges[node.name][keyPath]);
                else
                  node.keyRanges[keyPath] = keyRanges[node.name][keyPath];
              }
            }
          }
        }
      }
    },

    Join: {
      enter(path) {
        let { node } = path;
        
        let lSchema = node.lRelation.schema();
        let rSchema = node.rRelation.schema();

        if (node.type !== "inner") {
          let terms = [];
          available.terms = available.terms.filter(term => {
            let dependencies = term.dependencies;
            if (term.keyRanges() !== undefined)
              return true;

            let rDepends = false;
            for (let n in dependencies) {
              if (has.call(dependencies, n))
                rDepends = rDepends || dependencies[n].isSameDependency(rSchema[n]);
            }

            if (rDepends)
              terms.push(term);
            return !rDepends;
          });

          if (terms.length) {
            let mergedTerm = terms.reduce((merged, term) => merged.merge(term))
            node.predicates.push(mergedTerm.expression());
          }
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

  if (available.terms.length !== 0)
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
