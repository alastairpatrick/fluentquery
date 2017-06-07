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

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "a",
      },
      {
        dependencies: ["b"],
        expression: "b",
      },
    ]);
  })

  it("finds terms of two logical && expressions", function() {
    groups.parse("a && b && c", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "a",
      },
      {
        dependencies: ["b"],
        expression: "b",
      },
      {
        dependencies: ["c"],
        expression: "c",
      },
    ]);
  })

  it("combines terms with the same dependencies", function() {
    groups.parse("a.x && b && a.y", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "a.x",
      },
      {
        dependencies: ["b"],
        expression: "b",
      },
      {
        dependencies: ["a"],
        expression: "a.y",
      },
    ]);
  })

  it("treats arithmetic expression as term", function() {
    groups.parse("a + b", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a", "b"],
        expression: "a + b",
      },
    ]);
  });

  it("treats logical || as term", function() {
    groups.parse("a || b", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a", "b"],
        expression: "a || b",
      },
    ]);
  });

  it("does not consider locally declared variables to be dependencies", function() {
    groups.parse("(c => c + b)(a)", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a", "b"],
        expression: "(c => c + b)(a)",
      },
    ]);
  });

  it("transforms equality on unbound identifier to cmp()", function() {
    groups.parse("a.x == 1", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "$$cmp(a.x, 1) === 0",
        keys: {
          a: {
            x: {
              class: "RangeExpression",
              lower: "1",
              upper: "1",
            },
          },
        }
      },
    ]);
  });

  it("transforms range on unbound identifier to cmp()", function() {
    groups.parse("a.x >= 1 && a.x < 2", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "$$cmp(a.x, 1) >= 0",
        keys: {
          a: {
            x: {
              class: "RangeExpression",
              lower: "1",
            },
          },
        }
      },
      {
        dependencies: ["a"],
        expression: "$$cmp(a.x, 2) < 0",
        keys: {
          a: {
            x: {
              class: "RangeExpression",
              upper: "2",
              upperOpen: true,
            },
          },
        }
      },
    ]);
  });

  it("both sides of comparison may depend on tuple 1", function() {
    groups.parse("a.x >= a.y", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "$$cmp(a.x, a.y) >= 0",
        keys: {
          a: {
            x: {
              class: "RangeExpression",
              lower: "a.y",
            },
            y: {
              class: "RangeExpression",
              upper: "a.x",
              upperOpen: true,
            },
          }
        }
      },
    ]);
  });

  it("both sides of comparison may depend on tuple 2", function() {
    groups.parse("a.x >= b.y", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a", "b"],
        expression: "$$cmp(a.x, b.y) >= 0",
        keys: {
          a: {
            x: {
              class: "RangeExpression",
              lower: "b.y",
            },
          },
          b: {
            y: {
              class: "RangeExpression",
              upper: "a.x",
              upperOpen: true,
            },
          }
        }
      },
    ]);
  });

  it("transforms equality on bound identifier to cmp()", function() {
    groups.parse("(v => v.x == 1)(a)", schema);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "(v => $$cmp(v.x, 1) === 0)(a)",
      },
    ]);
  });

  it("throws if dependency is not present in schema", function() {
    expect(function() {
      groups.parse("a && bad", schema);
    }).to.throw(/bad/);
  })

  it("throws if unbound identifier begins with $$", function() {
    expect(function() {
      groups.parse("$$bad.x", schema);
    }).to.throw(/bad/);
  })

  it("compiles expression with substitution", function() {
    groups.parse(["a == ", " && b"], schema, [7]);

    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "$$cmp(a, $$subs[0]) === 0",
      },
      {
        dependencies: ["b"],
        expression: "b",
      },
    ]);
    expect(groups.substitutions).to.deep.equal([7]);
  })

  it("merges groups", function() {
    groups.parse("a && b", schema);

    let groups2 = new TermGroups();
    groups2.parse("a && c", schema);
    groups.merge(groups2);
    
    let terms = groups.terms;
    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "a",
      },
      {
        dependencies: ["b"],
        expression: "b",
      },
      {
        dependencies: ["a"],
        expression: "a",
      },
      {
        dependencies: ["c"],
        expression: "c",
      },
    ]);
  })

  it("merges groups with substitutions", function() {
    groups.parse(["a + ", " > 1"], schema, [7]);

    let groups2 = new TermGroups();
    groups2.parse(["a + ", " < 2"], schema, [8]);
    groups.merge(groups2);
    
    expect(groups.terms.map(t => t.tree())).to.deep.equal([
      {
        dependencies: ["a"],
        expression: "$$cmp(a + $$subs[0], 1) > 0",
      },
      {
        dependencies: ["a"],
        expression: "$$cmp(a + $$subs[1], 2) < 0",
      },
    ]);
    expect(groups.substitutions).to.deep.equal([7, 8]);
  })

  describe("ranges", function() {
    it("identifies == range", function() {
      groups.parse("a.x == 1", schema);      
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeExpression",
        lower: "1",
        upper: "1",
      }]);
    })

    it("identifies >= range", function() {
      groups.parse("a.x >= 1", schema);
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeExpression",
        lower: "1",
      }]);
    })

    it("identifies > range", function() {
      groups.parse("a.x > 1", schema);
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeExpression",
        lower: "1",
        lowerOpen: true,
      }]);
    })

    it("identifies <= range", function() {
      groups.parse("a.x <= 1", schema);
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeExpression",
        upper: "1",
      }]);
    })

    it("identifies < range", function() {
      groups.parse("a.x < 1", schema);
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeExpression",
        upper: "1",
        upperOpen: true,
      }]);
    })

    it("identifies == range on two keys", function() {
      groups.parse("a.x == b.x", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              lower: "b.x",
              upper: "b.x",
            },
          },
          b: {
            x: {
              class: "RangeExpression",
              lower: "a.x",
              upper: "a.x",
            },
          },
        }
      ]);
    })

    it("identifies >= range on two keys", function() {
      groups.parse("a.x >= b.x", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              lower: "b.x",
            },
          },
          b: {
            x: {
              class: "RangeExpression",
              upper: "a.x",
              upperOpen: true,
            },
          },
        }
      ]);
    })

    it("identifies > range on two keys", function() {
      groups.parse("a.x > b.x", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              lower: "b.x",
              lowerOpen: true,
            },
          },
          b: {
            x: {
              class: "RangeExpression",
              upper: "a.x",
            },
          },
        }
      ]);
    })

    it("identifies <= range on two keys", function() {
      groups.parse("a.x <= b.x", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              upper: "b.x",
            },
          },
          b: {
            x: {
              class: "RangeExpression",
              lower: "a.x",
              lowerOpen: true,
            },
          },
        }
      ]);
    })

    it("identifies < range on two keys", function() {
      groups.parse("a.x < b.x", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              upper: "b.x",
              upperOpen: true,
            },
          },
          b: {
            x: {
              class: "RangeExpression",
              lower: "a.x",
            },
          },
        }
      ]);
    })

    it("identifies union", function() {
      groups.parse("a.x == 1 || a.x == 2", schema);
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
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
      }]);
    })

    it("does not identify top level intersection", function() {
      groups.parse("a.x >= 1 && a.x <= 2", schema);
      let terms = groups.terms;
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([
        {
          class: "RangeExpression",
          lower: "1",
        },
        {
          class: "RangeExpression",
          upper: "2",
        },
      ]);
    })

    it("identifies complement of && expression and transforms to union", function() {
      groups.parse("!(a.x >= 1 && a.x <= 2)", schema);
      let terms = groups.terms;
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
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
      }]);
    })

    it("identifies complement of || expression and transforms to intersection", function() {
      groups.parse("!(a.x < 1 || a.x > 2)", schema);
      let terms = groups.terms;
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeIntersection",
        left: {
          class: "RangeExpression",
          lower: "1",
        },
        right: {
          class: "RangeExpression",
          upper: "2",
        },
      }]);
    })

    it("does not identify complement of equality as range", function() {
      groups.parse("!(a.x == 1)", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([undefined]);
    })

    it("does not identify expression with non-range component as range", function() {
      groups.parse("a.x == 1 || Math.sin(a.x) < 0", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([undefined]);
      })

    it("does not identify expression with mixed key dependencies as range", function() {
      groups.parse("a.x == 1 || b.x == 1", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([undefined]);
    })

    it("does not identify expression with mixed key paths as range", function() {
      groups.parse("a.x == 1 || a.y == 1", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([undefined]);
    })

    it("identifies range with parameter", function() {
      groups.parse("a.x == $x", schema);
      expect(groups.terms.map(t => t.tree().keys.a.x)).to.deep.equal([{
        class: "RangeExpression",
        lower: "this.params.x",
        upper: "this.params.x",
      }]);
    })

    it("identifies separate range for each dependent", function() {
      groups.parse("a.x == 1 && b.x >= 10", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              lower: "1",
              upper: "1",
            },
          },
        },
        {
          b: {
            x: {
              class: "RangeExpression",
              lower: "10",
            },
          },
        }
      ]);
    })

    it("identifies separate ranges within each dependent", function() {
      groups.parse("a.x == 1 && a.y >= 10", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              lower: "1",
              upper: "1",
            },
          }
        },
        {
          a: {
            y: {
              class: "RangeExpression",
              lower: "10",
            },
          },
        }
      ]);
    })

    it("identifies intersection of range and another expression of same dependent", function() {
      groups.parse("a.x == 1 && a.x", schema);
      expect(groups.terms.map(t => t.tree().keys)).to.deep.equal([
        {
          a: {
            x: {
              class: "RangeExpression",
              lower: "1",
              upper: "1",
            },
          }
        },
        undefined
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
    expect(groups.terms[0].expression().tree()).to.equal("a + b");
  })

  it("can evaluate expression", function() {
    groups.parse("a + b", schema);
    let expression = groups.terms[0].expression();
    let prepared = expression.prepare(context);
    expect(prepared({ a: 1, b: 2 })).to.equal(3);
  })

  it("can customize expression global scope", function() {
    groups.parse("custom", schema);
    let expression = groups.terms[0].expression();
    let prepared = expression.prepare(context);
    expect(prepared()).to.equal("custom value");
  })

  it("can partially evaluate expression", function() {
    groups.parse("a + b", schema);
    let expression = groups.terms[0].expression();
    let partial = expression.partial({ a: 1 });
    expect(partial.dependencies).to.deep.equal({b});
    let prepared = partial.prepare(context);
    expect(prepared({ b: 2 })).to.equal(3);
  })

  it("evaluates with substitution", function() {
    groups.parse(["a + ", " + 1"], schema, [2]);
    let expression = groups.terms[0].expression();
    let prepared = expression.prepare(context);
    expect(prepared({ a: 1 })).to.equal(4);
  })

  it("parses expression with aggregate", function() {
    let expression = parseExpression(["sum(a.x)"], schema, [], {
      allowAggregates: true,
    });
    expect(expression.tree()).to.deep.equal("$$g[0] = sum($$g[0], a.x), $$g[0].value");
    expect(expression.dependencies).to.deep.equal({a});
  })

  it("schemaless expression can destructure input", function() {
    let expression = parseExpression(["a.x"], undefined, []);
    expect(expression.tree()).to.deep.equal("a.x");
    expect(expression.dependencies).to.deep.equal({a: unknownDependency});
  })
})
