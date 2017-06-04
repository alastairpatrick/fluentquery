"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");

const {
  Context,
  Expression,
  TermGroups,
  expressionScope,
  parseExpression,
  unknownDependency,
} = require("..");

describe("TermGroups", function() {
  let a = 1, b = 2, c = 3;
  let schema = { a, b, c };
  let groups;

  beforeEach(function() {
    groups = new TermGroups();
  })

  it("parses expressions", function() {
    let expression = parseExpression("a && b", schema);
    expect(expression.tree()).to.deep.equal("a && b");
    expect(expression.dependencies).to.deep.equal({a, b});
  })

  it("finds terms of one logical && expression", function() {
    groups.parse("a && b", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(2);
    expect(terms.get({a}).tree().expression).to.equal("a");
    expect(terms.get({b}).tree().expression).to.equal("b");
  })

  it("finds terms of two logical && expressions", function() {
    groups.parse("a && b && c", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(3);
    expect(terms.get({a}).tree().expression).to.equal("a");
    expect(terms.get({b}).tree().expression).to.equal("b");
    expect(terms.get({c}).tree().expression).to.equal("c");
  })

  it("combines terms with the same dependencies", function() {
    groups.parse("a.x && b && a.y", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(2);
    expect(terms.get({a}).tree().expression).to.equal("a.x && a.y");
    expect(terms.get({b}).tree().expression).to.equal("b");
  })

  it("treats arithmetic expression as term", function() {
    groups.parse("a + b", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a, b}).tree().expression).to.deep.equal("a + b");
  });

  it("treats logical || as term", function() {
    groups.parse("a || b", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a, b}).tree().expression).to.deep.equal("a || b");
  });

  it("does not consider locally declared variables to be dependencies", function() {
    groups.parse("(c => c + b)(a)", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a, b}).tree().expression).to.deep.equal("(c => c + b)(a)");
  });

  it("transforms equality on unbound identifier to cmp()", function() {
    groups.parse("a.x == 1", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a}).tree().expression).to.equal("$cmp(a.x, 1) === 0");
  });

  it("transforms range on unbound identifier to cmp()", function() {
    groups.parse("a.x >= 1 && a.x < 2", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a}).tree().expression).to.equal("$cmp(a.x, 1) >= 0 && $cmp(a.x, 2) < 0");
  });

  it("both sides of comparison may depend on tuple", function() {
    groups.parse("a.x >= a.y", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a}).tree().expression).to.equal("$cmp(a.x, a.y) >= 0");
  });

  it("transforms equality on bound identifier to cmp()", function() {
    groups.parse("(v => v.x == 1)(a)", schema);

    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a}).tree().expression).to.equal("(v => $cmp(v.x, 1) === 0)(a)");
  });

  it("throws if dependency is not present in schema", function() {
    expect(function() {
      groups.parse("a && bad", schema);
    }).to.throw(/bad/);
  })

  it("throws if unbound identifier begins with $", function() {
    expect(function() {
      groups.parse("$bad.x", schema);
    }).to.throw(/bad/);
  })

  it("compiles expression with substitution", function() {
    groups.parse(["a == ", " && b"], schema, [7]);

    let terms = groups.terms;
    expect(terms.size).to.equal(2);
    expect(terms.get({a}).tree().expression).to.equal("$cmp(a, $subs[0]) === 0");
    expect(terms.get({b}).tree().expression).to.equal("b");
    expect(groups.substitutions).to.deep.equal([7]);
  })

  it("merges groups", function() {
    groups.parse("a && b", schema);

    let groups2 = new TermGroups();
    groups2.parse("a && c", schema);
    groups.merge(groups2);
    
    let terms = groups.terms;
    expect(terms.size).to.equal(3);
    expect(terms.get({a}).tree().expression).to.equal("a && a");
    expect(terms.get({b}).tree().expression).to.equal("b");
    expect(terms.get({c}).tree().expression).to.equal("c");
  })

  it("merges groups with substitutions", function() {
    groups.parse(["a + ", " > 1"], schema, [7]);

    let groups2 = new TermGroups();
    groups2.parse(["a + ", " < 2"], schema, [8]);
    groups.merge(groups2);
    
    let terms = groups.terms;
    expect(terms.size).to.equal(1);
    expect(terms.get({a}).tree().expression).to.equal("$cmp(a + $subs[0], 1) > 0 && $cmp(a + $subs[1], 2) < 0");
    expect(groups.substitutions).to.deep.equal([7, 8]);
  })

  describe("ranges", function() {
    it("identifies == range", function() {
      groups.parse("a.x == 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "1",
        upper: "1",
      });
    })

    it("identifies >= range", function() {
      groups.parse("a.x >= 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "1",
      });
    })

    it("identifies > range", function() {
      groups.parse("a.x > 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "1",
        lowerOpen: true,
      });
    })

    it("identifies <= range", function() {
      groups.parse("a.x <= 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        upper: "1",
      });
    })

    it("identifies < range", function() {
      groups.parse("a.x < 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        upper: "1",
        upperOpen: true,
      });
    })

    it("identifies == range on two keys", function() {
      groups.parse("a.x == b.x", schema);
      let terms = groups.terms;
      expect(terms.get({a, b}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "b.x",
        upper: "b.x",
      });
      expect(terms.get({a, b}).tree().keys.b.x).to.deep.equal({
        class: "RangeExpression",
        lower: "a.x",
        upper: "a.x",
      });
    })

    it("identifies >= range on two keys", function() {
      groups.parse("a.x >= b.x", schema);
      let terms = groups.terms;
      expect(terms.get({a, b}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "b.x",
      });
      expect(terms.get({a, b}).tree().keys.b.x).to.deep.equal({
        class: "RangeExpression",
        upper: "a.x",
        upperOpen: true,
      });
    })

    it("identifies > range on two keys", function() {
      groups.parse("a.x > b.x", schema);
      let terms = groups.terms;
      expect(terms.get({a, b}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "b.x",
        lowerOpen: true,
      });
      expect(terms.get({a, b}).tree().keys.b.x).to.deep.equal({
        class: "RangeExpression",
        upper: "a.x",
      });
    })

    it("identifies <= range on two keys", function() {
      groups.parse("a.x <= b.x", schema);
      let terms = groups.terms;
      expect(terms.get({a, b}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        upper: "b.x",
      });
      expect(terms.get({a, b}).tree().keys.b.x).to.deep.equal({
        class: "RangeExpression",
        lower: "a.x",
        lowerOpen: true,
      });
    })

    it("identifies < range on two keys", function() {
      groups.parse("a.x < b.x", schema);
      let terms = groups.terms;
      expect(terms.get({a, b}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        upper: "b.x",
        upperOpen: true,
      });
      expect(terms.get({a, b}).tree().keys.b.x).to.deep.equal({
        class: "RangeExpression",
        lower: "a.x",
      });
    })

    it("identifies union", function() {
      groups.parse("a.x == 1 || a.x == 2", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeUnion",
        left: {
          class: "RangeExpression",
          lower: "1",
          upper: "1",
        },
        right: {
          class: "RangeExpression",
          lower: "2",
          upper: "2",
        },
      });
    })

    it("identifies intersection", function() {
      groups.parse("a.x >= 1 && a.x <= 2", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeIntersection",
        left: {
          class: "RangeExpression",
          lower: "1",
        },
        right: {
          class: "RangeExpression",
          upper: "2",
        },
      });
    })

    it("identifies complement of && expression and transforms to union", function() {
      groups.parse("!(a.x >= 1 && a.x <= 2)", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeUnion",
        left: {
          class: "RangeExpression",
          upper: "1",
          upperOpen: true,
        },
        right: {
          class: "RangeExpression",
          lower: "2",
          lowerOpen: true,
        },
      });
    })

    it("identifies complement of || expression and transforms to intersection", function() {
      groups.parse("!(a.x < 1 || a.x > 2)", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeIntersection",
        left: {
          class: "RangeExpression",
          lower: "1",
        },
        right: {
          class: "RangeExpression",
          upper: "2",
        },
      });
    })

    it("does not identify complement of equality as range", function() {
      groups.parse("!(a.x == 1)", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys).to.be.undefined;
    })

    it("does not identify expression with non-range component as range", function() {
      groups.parse("a.x == 1 || $p.something", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys).to.be.undefined;
    })

    it("does not identify expression with mixed key dependencies as range", function() {
      groups.parse("a.x == 1 || b.x == 1", schema);
      let terms = groups.terms;
      expect(terms.get({a, b}).tree().keys).to.be.undefined;
    })

    it("does not identify expression with mixed key paths as range", function() {
      groups.parse("a.x == 1 || a.y == 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys).to.be.undefined;
    })

    it("identifies range with parameter", function() {
      groups.parse("a.x == $p.x", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "$p.x",
        upper: "$p.x",
      });
    })

    it("evaluates range expression", function() {
      groups.parse("a.x == 1 + 1", schema);
      let terms = groups.terms;
      expect(terms.get({a}).tree().keys.a.x).to.deep.equal({
        class: "RangeExpression",
        lower: "1 + 1",
        upper: "1 + 1",
      });
      expect(terms.get({a}).keyRanges().a.x.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 2,
          upper: 2,
        }        
      ]);
    })
  })
})

describe("Expression", function() {
  let a = "a", b = "b", c = "c";
  let schema = { a, b, c };
  let groups;
  let context;

  beforeEach(function() {
    groups = new TermGroups();
    context = new Context();

    expressionScope.custom = "custom value";
  })

  afterEach(function() {
    delete expressionScope.custom;
  })

  it("tree() evaluates to source", function() {
    groups.parse("a + b", schema);
    let terms = groups.terms;
    expect(terms.get({a, b}).expression().tree()).to.equal("a + b");
  })

  it("can evaluate expression", function() {
    groups.parse("a + b", schema);
    let terms = groups.terms;
    let expression = terms.get({a, b}).expression();
    let prepared = expression.prepare(context);
    expect(prepared({ a: 1, b: 2 })).to.equal(3);
  })

  it("can customize expression global scope", function() {
    groups.parse("custom", schema);
    let terms = groups.terms;
    let expression = terms.get({}).expression();
    let prepared = expression.prepare(context);
    expect(prepared()).to.equal("custom value");
  })

  it("can partially evaluate expression", function() {
    groups.parse("a + b", schema);
    let terms = groups.terms;
    let expression = terms.get({a, b}).expression();
    let partial = expression.partial({ a: 1 });
    expect(partial.dependencies).to.deep.equal({b});
    let prepared = partial.prepare(context);
    expect(prepared({ b: 2 })).to.equal(3);
  })

  it("evaluates with substitution", function() {
    groups.parse(["a + ", " + 1"], schema, [2]);
    let terms = groups.terms;
    let expression = terms.get({a}).expression();
    let prepared = expression.prepare(context);
    expect(prepared({ a: 1 })).to.equal(4);
  })

  it("parses expression with aggregate", function() {
    let expression = parseExpression(["sum(a.x)"], schema, [], {
      allowAggregates: true,
    });
    expect(expression.tree()).to.deep.equal("$g[0] = sum($g[0], a.x), $g[0].value");
    expect(expression.dependencies).to.deep.equal({a});
  })

  it("schemaless expression can destructure input", function() {
    let expression = parseExpression(["a.x"], undefined, []);
    expect(expression.tree()).to.deep.equal("a.x");
    expect(expression.dependencies).to.deep.equal({a: unknownDependency});
  })
})
