"use strict";

class Path {
  constructor(parent, childName, parentPath) {
    this.parent = parent;
    this.childName = childName;
    this.parentPath = parentPath;
  }

  get node() {
    return this.parent[this.childName];
  }
  
  replaceWith(node) {
    this.parent[this.childName] = node;
  }
}

const enter = (context) => {
  let visitor = context.visitor;
  let path = context.path;
  let node = path.node;
  if (node && typeof node === "object") {
    let type = node.constructor.name;
    let handler = visitor[type];
    if (typeof handler === "function")
      handler.call(visitor, path, context);
    else if (typeof handler === "object" && typeof handler.enter === "function")
      handler.enter.call(visitor, path, context);
  }

  if (typeof visitor.enter === "function")
    visitor.enter(path, context);
}

const exit = (context) => {
  let visitor = context.visitor;
  let path = context.path;
  if (typeof visitor.exit === "function")
    visitor.exit(path, context);

  let node = path.node;
  if (node && typeof node === "object") {
    let type = node.constructor.name;
    let handler = visitor[type];
    if (typeof handler === "object" && typeof handler.exit === "function")
      handler.exit.call(visitor, path, context);
  }
}

const traversePath = (obj, name, parentContext) => {
  let path = new Path(obj, name, parentContext.path);
  let context = { path, visitor: parentContext.visitor };
  enter(context);
  let node = path.node;
  if (node && typeof node === "object" && typeof node.accept === "function")
    node.accept(context);
  exit(context);
}

const traverse = (node, visitor) => {
  let fakeParent = [node];
  let context = { path: undefined, visitor };
  traversePath(fakeParent, 0, context);
  return fakeParent[0];
}


module.exports = {
  traverse,
  traversePath
};
