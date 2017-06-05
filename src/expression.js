"use strict";

const isEqualWith = require("lodash/isEqualWith");
const { babylon, traverse, types, generate } = require("babel-to-go");
const { ValueMap } = require("valuecollection");

const { stdAggregates } = require("./aggregate");
const { cmp } = require("./idbbase");
const { Range, RangeExpression, RangeIntersection, RangeUnion, includes } = require("./range");

const has = Object.prototype.hasOwnProperty;

const RANGE_OPS = {
  "==": "===",
  ">=": ">=",
  ">": ">",
  "<=": "<=",
  "<": "<",
};

const RESERVED_IDS = {
  "$$g": true,
  "$$subs": true,
  "$$this": true,
}

const expressionScope = {
  cmp,
  $$cmp: cmp,
}
expressionScope.global = expressionScope;
Object.assign(expressionScope, stdAggregates);

class DependencyMap extends ValueMap {
  isEqual(a, b) {
    return isEqualWith(a, b, (a, b) => {
      if (a && typeof a === "object" && typeof a.isSameDependency === "function")
        return a.isSameDependency(b);
    });
  }
}

const unknownDependency = {
  isSameDependency(that) {
    return false;
  }
}

const generateJS = (node) => {
  let { code } = generate(node, { concise: true });
  return code;
}

const compileNode = (node, dependencies, substitutions) => {
  let js = generateJS(node);

  let jsTuple = "";
  let sep = "";
  for (let n in dependencies) {
    if (has.call(dependencies, n)) {
      jsTuple += sep + n;
      sep = ", ";
    }
  }

  let jsWrapped = "return function(";

  if (jsTuple.length)
    jsWrapped += '{' + jsTuple + '}';
  else
    jsWrapped += "$$_";

  jsWrapped += ", $$g) { return " + js + " }";

  let parameters = Object.keys(expressionScope);
  let args = parameters.map(n => expressionScope[n]);

  let fn = new Function("$$subs", ...parameters, jsWrapped)(substitutions, ...args);
  fn.source = js;
  return fn;
}

class Expression {
  constructor(fn, dependencies, tuple) {
    this.fn = fn;
    this.dependencies = dependencies;
    this.tuple = tuple;
  }

  prepare(context) {
    if (this.tuple)
      return (tuple, group) => this.fn.call(context, Object.assign({}, this.tuple, tuple), group);
    else
      return (tuple, group) => this.fn.call(context, tuple, group);
  }

  partial(tuple) {
    let newDependencies = Object.assign({}, this.dependencies);
    for (let n in tuple) {
      if (has.call(tuple, n))
        delete newDependencies[n];
    }

    return new Expression(this.fn, newDependencies, Object.assign({}, this.tuple, tuple));
  }

  expandedTree() {
    let result = {
      class: this.constructor.name,
      dependencies: Object.getOwnPropertyNames(this.dependencies).sort(),
      source: this.fn.source || this.fn.toString(),
    };

    if (this.tuple !== undefined)
      result.tuple = this.tuple;

    return result;
  }

  tree() {
    return this.fn.source || this.fn.toString();
  }
}

const extractKeyPath = (node) => {
  let keyPath;
  while (types.isMemberExpression(node)) {
    if (!types.isIdentifier(node.property))
      return undefined;

    if (keyPath)
      keyPath = node.property.name + '.' + keyPath;
    else
      keyPath = node.property.name;
    node = node.object;
  }

  if (!types.isIdentifier(node))
    return undefined;

  if (node.name[0] === '$')
    return undefined;

  return {
    path: keyPath,
    dependency: node.name,
  };
}

const extractKeyRanges = (node, complement, dependencies) => {
  let result = {};
  let op = node.operator;
  if (types.isUnaryExpression(node, { operator: '!' })) {
    return extractKeyRanges(node.argument, !complement, dependencies);
  } else if (types.isLogicalExpression(node)) {
    if (op !== "&&" && op !== "||") {
      return undefined;
    } else {
      let left = extractKeyRanges(node.left, complement, dependencies)
      let right = extractKeyRanges(node.right, complement, dependencies)
      if (left === undefined || right === undefined)
        return undefined;

      for (let keyDependency in left) {
        if (has.call(left, keyDependency) && has.call(right, keyDependency)) {
          for (let keyPath in left[keyDependency]) {
            if (has.call(left[keyDependency], keyPath) && has.call(right[keyDependency], keyPath)) {
              if (!has.call(result, keyDependency))
                result[keyDependency] = {};

              let intersect = op === "&&";
              if (complement)
                intersect = !intersect;

              result[keyDependency][keyPath] = intersect
                ? new RangeIntersection(left[keyDependency][keyPath], right[keyDependency][keyPath])
                : new RangeUnion(left[keyDependency][keyPath], right[keyDependency][keyPath]);
            }
          }
        }
      }
    }
  } else if (types.isBinaryExpression(node)) {
    if (op !== "===" && op !== ">=" && op !== ">" && op !== "<=" && op !== "<") {
      return undefined;
    } else {
      if (!types.isCallExpression(node.left))
        return undefined;
      if (!types.isIdentifier(node.left.callee, { name: "$$cmp" }))
        return undefined;
      if (!types.isNumericLiteral(node.right, { value: 0 }))
        return undefined;

      let args = node.left.arguments;

      for (let i = 0; i < 2; ++i) {
        let key = extractKeyPath(args[i]);
        if (key === undefined)
          continue;

        let keyRangeDependencies = Object.assign({}, dependencies);
        delete keyRangeDependencies[key.dependency];

        let fn = compileNode(args[1 - i], keyRangeDependencies);
        let keyRange = new RangeExpression(fn, fn);

        if (op === "===") {
          if (complement)
            continue;
        } else {
          if (complement ^ (i === 1)) {
            if (op === ">=" || op === ">") {
              keyRange.lowerFn = undefined;
              keyRange.lowerSource = undefined;
            } else if (op === "<=" || op === "<") {
              keyRange.upperFn = undefined;
              keyRange.upperSource = undefined;
            }
            
            if (op === ">=") {
              keyRange.upperOpen = true;
            }
            
            if (op === "<=") {
              keyRange.lowerOpen = true;
            }
          } else {
            if (op === ">=" || op === ">") {
              keyRange.upperFn = undefined;
              keyRange.upperSource = undefined;
            } else if (op === "<=" || op === "<") {
              keyRange.lowerFn = undefined;
              keyRange.lowerSource = undefined;
            }
            
            if (op === ">") {
              keyRange.lowerOpen = true;
            }
            
            if (op === "<") {
              keyRange.upperOpen = true;
            }
          }
        }

        if (!has.call(result, key.dependency))
          result[key.dependency] = {};
        result[key.dependency][key.path] = keyRange;
      }
    }
  }

  return result;
}

class ExpressionKeys {
  constructor(expression, keys) {
    this.expression = expression;
    this.keys = keys;
  }

  tree() {
    let result = {
      expression: this.expression.tree(),
    }
    for (let d in this.keys) {
      if (has.call(this.keys, d)) {
        if (!has.call(result, "keys"))
          result.keys = {};
        if (!has.call(result.keys, d))
          result.keys[d] = {};
        for (let k in this.keys[d]) {
          if (has.call(this.keys[d], k))
            result.keys[d][k] = this.keys[d][k].tree();
        }
      }
    }
    return result;
  }
}

class Term {
  constructor(node, dependencies, substitutions) {
    this.node = node;
    this.dependencies = dependencies;
    this.substitutions = substitutions;
    this.expression_ = undefined;
    this.keyRanges_ = undefined;
  }

  // Dependencies and substitutions must be compatible.
  merge(other) {
    this.node = types.logicalExpression("&&", this.node, other.node);
    this.expression_ = undefined;
    this.keyRanges_ = undefined;
  }

  expression() {
    if (this.expression_ !== undefined)
      return this.expression_;

    let fn = compileNode(this.node, this.dependencies, this.substitutions);
    this.expression_ = new Expression(fn, this.dependencies);
    return this.expression_;
  }

  keyRanges() {
    if (this.keyRanges_ !== undefined)
      return this.keyRanges_;

    this.keyRanges_ = extractKeyRanges(this.node, false, this.dependencies)
    return this.keyRanges_;
  }

  tree() {
    let result = {
      dependencies: Object.getOwnPropertyNames(this.dependencies).sort(),
      expression: this.expression().tree(),
    };

    let keys = this.keyRanges();
    for (let d in keys) {
      if (has.call(keys, d)) {
        if (!has.call(result, "keys"))
          result.keys = {};
        for (let k in keys[d]) {
          if (has.call(keys[d], k)) {
            if (!has.call(result.keys, d))
              result.keys[d] = {};
            result.keys[d][k] = keys[d][k].tree();
          }
        }
      }
    }

    return result;
  }
}

const groupExpression = (groupIdx) => {
  return types.memberExpression(types.identifier("$$g"), types.numericLiteral(groupIdx), true);
}

class TermGroups {
  constructor() {
    this.terms = new DependencyMap();
    this.substitutions = [];
    this.substitutionNodes = [];
  }

  merge(other) {
    other.substitutionNodes.forEach(node => {
      node.value += this.substitutions.length;
    });
    this.substitutions = this.substitutions.concat(other.substitutions);
    this.substitutionNodes = this.substitutionNodes.concat(other.substitutionNodes);

    for (let [dependencies, otherTerm] of other.terms.entries()) {
      let thisTerm = this.terms.get(dependencies);
      if (thisTerm === undefined) {
        this.terms.set(dependencies, otherTerm);
      } else {
        thisTerm.merge(otherTerm);
      }
    }

    other.terms = other.substitutions = other.substitutionNodes = undefined;
  }

  parse(template, schema, substitutions, options={}) {
    let js = "";
    if (Array.isArray(template) && Array.isArray(substitutions)) {
      for (let i = 0; i < template.length; ++i) {
        js += template[i];
        if (i < substitutions.length)
          js += ` ($$subs[${this.substitutions.length + i}]) `;
      }
      this.substitutions = this.substitutions.concat(substitutions);
    } else {
      js = template;
    }

    let ast = babylon.parse("(" + js + ")");

    let generated = new Set();  
    let initializers = [];
    let termDependencies = {};
    let allDependencies = {};
    let termNode;

    const isTermRoot = (path) => {
      if (path.isLogicalExpression({ operator: "&&" }))
        return false;
      let parentNonAnd = path.findParent(path => !path.isLogicalExpression({ operator: "&&" }))
      if (!parentNonAnd.isExpressionStatement() || !parentNonAnd.parentPath.isProgram())
        return false;
      return true;
    }

    let groupIdx = 0;
    traverse(ast, {
      MemberExpression(path) {
        if (path.get("object").isIdentifier({ name: "$$subs" })) {
          let subsPath = path.get("property");
          types.assertNumericLiteral(subsPath.node);
          this.substitutionNodes.push(subsPath.node);
        }
      },

      BinaryExpression(path) {
        let { node } = path;
        let op = RANGE_OPS[node.operator];
        if (op !== undefined && !generated.has(node)) {
          let replacement = types.binaryExpression(op,
            types.callExpression(types.identifier("$$cmp"), [node.left, node.right]),
            types.numericLiteral(0));

          generated.add(replacement);
          path.replaceWith(replacement);
        }
      },

      CallExpression: {
        exit(path) {
          let { node, scope } = path;
          let callee = path.get("callee");
          if (callee.isIdentifier() && !scope.hasBinding(callee.node.name)) {
            let fn = expressionScope[callee.node.name];
            if (typeof expressionScope[callee.node.name] === "function" && fn.isAggregate) {
              if (!options.allowAggregates)
                throw new Error(`Call to aggregage '${callee.node.name}' is not allowed in this context.`);

              initializers.push(
                types.assignmentExpression(
                  "=",
                  groupExpression(groupIdx),
                  types.callExpression(types.identifier(callee.node.name), [groupExpression(groupIdx)].concat(node.arguments))
                )
              );

              path.replaceWith(types.memberExpression(groupExpression(groupIdx), types.identifier("value")));
              ++groupIdx;
            }
          }
        }
      },

      ThisExpression(path) {
        if (!generated.has(path.node))
          path.replaceWith(types.identifier("$$this"));
      },

      Expression: {
        enter(path) {
          let { node, scope } = path;

          if (isTermRoot(path)) {
            termDependencies = {};
            termNode = node;
          }

          if (path.isReferencedIdentifier()) {
            if (!scope.hasBinding(node.name)) {
              if (node.name[0] === "$" && node.name !== "$$this") {
                if (node.name[1] === "$") {
                  if (!has.call(RESERVED_IDS, node.name) && !has.call(expressionScope, node.name))
                    throw new Error(`Variables beginning '$$' are reserved but found '${node.name}'.`);
                } else {
                  let thisExpression = types.thisExpression();
                  generated.add(thisExpression);
                  path.replaceWith(types.memberExpression(
                    types.memberExpression(thisExpression, types.identifier("params")),
                    types.identifier(node.name.substring(1))));
                }
              } else if(!has.call(expressionScope, node.name)) {
                if (schema !== undefined) {
                  if (!has.call(schema, node.name))
                    throw new Error(`No relation matching variable "${node.name}".`);
                  allDependencies[node.name] = termDependencies[node.name] = schema[node.name];
                } else {
                  allDependencies[node.name] = termDependencies[node.name] = unknownDependency;
                }
              }
            }
          }
        },

        exit(path) {
          let { node } = path;
          if (node === termNode) {
            let newTerm = new Term(termNode, termDependencies, this.substitutions);
            let term = this.terms.get(termDependencies);
            if (term === undefined)
              this.terms.set(termDependencies, newTerm);
            else
              term.merge(newTerm);
            termNode = undefined;
          }
        }
      },
    }, undefined, this);

    if (this.substitutions.length !== this.substitutionNodes.length)
      throw new Error("Substitutions out-of-sync");
    
    if (options.compileAll) {
      let expression = ast.program.body[0].expression;
      expression = types.sequenceExpression(initializers.concat(expression));
      let fn = compileNode(expression, allDependencies, this.substitutions);
      return new Expression(fn, allDependencies);
    }
  }

  tree() {
    let result = [];
    for (let term of this.terms.values()) {
      result.push(term.tree());
    }
    return result.sort((a, b) => {
      if (a.expression < b.expression)
        return -1;
      else if (a.expression > b.expression)
        return 1;
      else
        return 0;
    });
  }
}

const parseExpression = (template, schema, substitutions, options) => {
  options = Object.assign({}, options, {
    compileAll: true,
  });
  let groups = new TermGroups();
  return groups.parse(template, schema, substitutions, options);
}

module.exports = {
  Expression,
  TermGroups,
  expressionScope,
  parseExpression,
  unknownDependency,
};
