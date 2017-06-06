"use strict";

require("./indexeddb-fill.js");

const { expect } = require("chai");
const sinon = require("sinon");

const { Context, Range, RangeExpression, RangeIntersection, RangeUnion, includes, nextUp } = require("..");


describe("Range", function() {
  let context;

  beforeEach(function() {
    context = new Context();
  })

  describe("Range", function() {
    it("executes finite range", function() {
      let range = new Range(1, 2, true, true);
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 2,
          lowerOpen: true,
          upperOpen: true,
        }
      ]);
    });

    it("executes range with unbounded upper", function() {
      let range = new Range(1, undefined, true, false);
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          lowerOpen: true,
        }
      ]);
    });

    it("executes range with unbounded lower", function() {
      let range = new Range(undefined, 1, false, true);
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          upperOpen: true,
        }
      ]);
    });

    it("executes closed range with equal lower and upper", function() {
      let range = new Range(1, 1, false, false);
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          lower: 1,
        }
      ]);
    });

    it("executes empty range", function() {
      let range = new Range(2, 1, true, true);
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([]);
    });

    it("does not include undefined", function() {
      let range = new Range(undefined, undefined);
      expect(range.includes(undefined)).to.be.false;
    })

    it("includes values when lower and upper unbounded", function() {
      let range = new Range(undefined, undefined);
      expect(range.includes(1)).to.be.true;
    })

    it("excludes value above upper", function() {
      let range = new Range(undefined, 1);
      expect(range.includes(2)).to.be.false;
    })

    it("includes value at closed upper", function() {
      let range = new Range(undefined, 1);
      expect(range.includes(1)).to.be.true;
    })

    it("excludes value at open upper", function() {
      let range = new Range(undefined, 1, false, true);
      expect(range.includes(1)).to.be.false;
    })

    it("includes value below upper", function() {
      let range = new Range(undefined, 2);
      expect(range.includes(1)).to.be.true;
    })

    it("excludes value below lower", function() {
      let range = new Range(1, undefined);
      expect(range.includes(0)).to.be.false;
    })

    it("includes value at closed lower", function() {
      let range = new Range(1, undefined);
      expect(range.includes(1)).to.be.true;
    })

    it("excludes value at open lower", function() {
      let range = new Range(1, undefined, true, false);
      expect(range.includes(1)).to.be.false;
    })

    it("includes value above lower", function() {
      let range = new Range(1, undefined);
      expect(range.includes(2)).to.be.true;
    })

    it("tests value against list of ranges", function() {
      let ranges = [
        new Range(1, 3),
        new Range(5, 7),
      ];
      let testFn = includes(ranges);
      expect(testFn(0)).to.be.false;
      expect(testFn(2)).to.be.true;
      expect(testFn(4)).to.be.false;
      expect(testFn(6)).to.be.true;
      expect(testFn(8)).to.be.false;
    })
  })

  describe("RangeExpression", function() {
    it("executes", function() {
      let range = new RangeExpression(({t}) => t.a, function({}) { return this.params.age }, true, true);
      context.params = { age: 18 };
      context.tuple = { t: { a: 1, b: 2 } };
      expect(range.tree()).to.deep.equal({
        class: "RangeExpression",
        lower: "({t}) => t.a",
        upper: "function ({}) { return this.params.age }",
        lowerOpen: true,
        upperOpen: true,
      });
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 18,
          lowerOpen: true,
          upperOpen: true,
        }
      ]);
    });

    it("lower may be unbounded", function() {
      let range = new RangeExpression(undefined, function({}) { return this.params.b }, false, false);
      context.params = {
        b: 2,
      };
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 2,
        }
      ]);
    });

    it("upper may be unbounded", function() {
      let range = new RangeExpression(function({}) { return this.params.a }, undefined);
      context.params = {
        a: 1,
      };
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
        }
      ]);
    });
  })

  describe("RangeIntersection", function() {
    it("executes intersection of range with itself", function() {
      let range = new RangeIntersection(
        new Range(1, 2),
        new Range(1, 2));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 2,
        }
      ]);
    });

    it("executes intersection of lower and upper unbounded range with itself", function() {
      let range = new RangeIntersection(
        new Range(undefined, undefined),
        new Range(undefined, undefined));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
        }
      ]);
    });

    it("executes intersection of non-overlapping ranges", function() {
      let range = new RangeIntersection(
        new Range(1, 2),
        new Range(3, 4));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([]);
    });

    it("executes intersection of left lower and right upper bound", function() {
      let range = new RangeIntersection(
        new Range(1, undefined, true, false),
        new Range(undefined, 2, false, true));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 2,
          lowerOpen: true,
          upperOpen: true,
        }
      ]);
    });

    it("executes intersection of left upper and right lower bound", function() {
      let range = new RangeIntersection(
        new Range(undefined, 2, false, true),
        new Range(1, undefined, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 2,
          lowerOpen: true,
          upperOpen: true,
        }
      ]);
    });


    it("executes intersection where lower left contains right", function() {
      let range = new RangeIntersection(
        new Range(1, undefined, true, false),
        new Range(2, undefined, false, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 2,
        }
      ]);
    });

    it("executes intersection where lower closed left contains open right", function() {
      let range = new RangeIntersection(
        new Range(1, undefined, false, false),
        new Range(1, undefined, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          lowerOpen: true,
        }
      ]);
    });

    it("executes intersection where lower right contains left", function() {
      let range = new RangeIntersection(
        new Range(2, undefined, false, false),
        new Range(1, undefined, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 2,
        }
      ]);
    });

    it("executes intersection where lower closed right contains open left", function() {
      let range = new RangeIntersection(
        new Range(1, undefined, true, false),
        new Range(1, undefined, false, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          lowerOpen: true,
        }
      ]);
    });


    it("executes intersection where upper left contains right", function() {
      let range = new RangeIntersection(
        new Range(undefined, 1, false, true),
        new Range(undefined, 2, false, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          upperOpen: true,
        }
      ]);
    });

    it("executes intersection where upper closed left contains open right", function() {
      let range = new RangeIntersection(
        new Range(undefined, 1, false, false),
        new Range(undefined, 1, false, true));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          upperOpen: true,
        }
      ]);
    });

    it("executes intersection where upper right contains left", function() {
      let range = new RangeIntersection(
        new Range(undefined, 2, false, false),
        new Range(undefined, 1, false, true));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          upperOpen: true,
        }
      ]);
    });

    it("executes intersection where upper closed right contains open left", function() {
      let range = new RangeIntersection(
        new Range(undefined, 1, false, true),
        new Range(undefined, 1, false, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          upperOpen: true,
        }
      ]);
    });

    it("executes intersection of non-intersecting left and right 1", function() {
      let range = new RangeIntersection(
        new Range(undefined, 1, false, true),
        new Range(2, undefined, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([]);
    });

    it("executes intersection of non-intersecting left and right 2", function() {
      let range = new RangeIntersection(
        new Range(undefined, 1, false, true),
        new Range(1, undefined, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([]);
    });

    it("executes intersection that results in equality", function() {
      let range = new RangeIntersection(
        new Range(undefined, 1, false, false),
        new Range(1, undefined, false, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 1,
        }
      ]);
    });

    it("executes empty intersection of string with array", function() {
      let range = new RangeIntersection(
        new Range("a", "b"),
        new Range([1], [2]));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([]);
    });
  })


  describe("RangeUnion", function() {
    it("executes union empty ranges", function() {
      let range = new RangeUnion(
        new Range(2, 1),
        new Range(2, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([]);
    });

    it("executes union of range with empty range", function() {
      let range = new RangeUnion(
        new Range(0, 1),
        new Range(2, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
        },
      ]);
    });

    it("executes union of empty range with range", function() {
      let range = new RangeUnion(
        new Range(2, 1),
        new Range(0, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
        },
      ]);
    });

    it("executes union of range with itself", function() {
      let range = new RangeUnion(
        new Range(0, 1),
        new Range(0, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
        },
      ]);
    });

    it("executes union of unbounded ranges 1", function() {
      let range = new RangeUnion(
        new Range(undefined, undefined),
        new Range(undefined, undefined));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
        },
      ]);
    });

    it("executes union of unbounded ranges 2 ", function() {
      let range = new RangeUnion(
        new Range(undefined, 1),
        new Range(undefined, undefined));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
        },
      ]);
    });

    it("executes union of unbounded ranges 3 ", function() {
      let range = new RangeUnion(
        new Range(undefined, undefined),
        new Range(undefined, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
        },
      ]);
    });

    it("executes union of right containing left 1", function() {
      let range = new RangeUnion(
        new Range(0, 1),
        new Range(0, 2));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 2,
        },
      ]);
    });

    it("executes union of right containing left 2", function() {
      let range = new RangeUnion(
        new Range(1, 2),
        new Range(0, 3));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 3,
        },
      ]);
    });

    it("executes union of right containing left 3", function() {
      let range = new RangeUnion(
        new Range(1, 2),
        new Range(0, 2));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 2,
        },
      ]);
    });

    it("executes union of left containing right 1", function() {
      let range = new RangeUnion(
        new Range(0, 2),
        new Range(0, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 2,
        },
      ]);
    });

    it("executes union of left containing right 2", function() {
      let range = new RangeUnion(
        new Range(0, 3),
        new Range(1, 2));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 3,
        },
      ]);
    });

    it("executes union of left containing right 3", function() {
      let range = new RangeUnion(
        new Range(0, 2),
        new Range(1, 2));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 2,
        },
      ]);
    });

    it("executes union of left before and overlapping right", function() {
      let range = new RangeUnion(
        new Range(0, 2),
        new Range(1, 3));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 3,
        },
      ]);
    });

    it("executes union of right before and overlapping left", function() {
      let range = new RangeUnion(
        new Range(1, 3),
        new Range(0, 2));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 3,
        },
      ]);
    });

    it("executes union of left completely before right", function() {
      let range = new RangeUnion(
        new Range(0, 1),
        new Range(2, 3));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
        },
        {
          class: "Range",
          lower: 2,
          upper: 3,
        },
      ]);
    });

    it("executes union of right completely before left", function() {
      let range = new RangeUnion(
        new Range(2, 3),
        new Range(0, 1));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
        },
        {
          class: "Range",
          lower: 2,
          upper: 3,
        },
      ]);
    });

    it("executes union of left just before right but disjoint", function() {
      let range = new RangeUnion(
        new Range(0, 1, false, true),
        new Range(1, 2, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
          upperOpen: true,
        },
        {
          class: "Range",
          lower: 1,
          upper: 2,
          lowerOpen: true,
        },
      ]);
    });

    it("executes union of right just before left but disjoint", function() {
      let range = new RangeUnion(
        new Range(1, 2, true, false),
        new Range(0, 1, false, true));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 1,
          upperOpen: true,
        },
        {
          class: "Range",
          lower: 1,
          upper: 2,
          lowerOpen: true,
        },
      ]);
    });

    it("executes union of left just before right", function() {
      let range = new RangeUnion(
        new Range(0, 1, false, false),
        new Range(1, 2, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 2,
        },
      ]);
    });

    it("executes union of right just before left", function() {
      let range = new RangeUnion(
        new Range(1, 2, false, false),
        new Range(0, 1, false, true));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 0,
          upper: 2,
        },
      ]);
    });

    it("executes union of disjoint unbounded ranges", function() {
      let range = new RangeUnion(
        new Range(undefined, 1, false, true),
        new Range(2, undefined, true, false));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          upper: 1,
          upperOpen: true,
        },
        {
          class: "Range",
          lower: 2,
          lowerOpen: true,
        }
      ]);
    });

    it("executes union of right straddline two sub-ranges of left", function() {
      let range = new RangeUnion(
        new RangeUnion(new Range(1, 3, false, true), new Range(3, 5, true, false)),
        new Range(2, 4));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: 1,
          upper: 5,
        }
      ]);
    });

    it("executes union of string < array", function() {
      let range = new RangeUnion(
        new Range("a", "b"),
        new Range([1], [2]));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: "a",
          upper: "b",
        },
        {
          class: "Range",
          lower: [1],
          upper: [2],
        }
      ]);
    });

    it("executes union of string < array", function() {
      let range = new RangeUnion(
        new Range("a", "b"),
        new Range([1], [2]));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: "a",
          upper: "b",
        },
        {
          class: "Range",
          lower: [1],
          upper: [2],
        }
      ]);
    });

    it("executes union of array > string", function() {
      let range = new RangeUnion(
        new Range([1], [2]),
        new Range("a", "b"));
      expect(range.prepare(context).map(r => r.tree())).to.deep.equal([
        {
          class: "Range",
          lower: "a",
          upper: "b",
        },
        {
          class: "Range",
          lower: [1],
          upper: [2],
        }
      ]);
    });
  })

  describe("nextUp", function() {
    it("returns the maximum negative representable finite number after -ve infinity", function() {
      expect(nextUp(-Infinity)).to.equal(-Number.MAX_VALUE);
    });

    it("returns the infinity after the maximum positive representable finite number", function() {
      expect(nextUp(Number.MAX_VALUE)).to.equal(Infinity);
    });

    it("returns the earliest representable date after infinity", function() {
      expect(nextUp(Infinity).getTime()).to.equal(-8640000000000000);
    });

    it("after a date, returns one millisecond later", function() {
      expect(nextUp(new Date(1000)).getTime()).to.equal(1001);
    });

    it("returns the empty string after the latest representable date", function() {
      expect(nextUp(new Date(8640000000000000))).to.equal("");
    });

    it("after any string, returns that string with a null character appended", function() {
      expect(nextUp("abc")).to.equal("abc\0");
    });

    it("after any array, returns that array with a -Infinity element appended", function() {
      expect(nextUp(["a", 7])).to.deep.equal(["a", 7, -Infinity]);
    });

    it("is used to implement Range.openUpper if its upper bound is closed", function() {
      expect(new Range("a", "b", false, false).openUpper().tree()).to.deep.equal({
        class: "Range",
        lower: "a",
        upper: "b\0",
        upperOpen: true,
      });
    });

    it("is used to implement Range.openUpper unless its upper bound is already open", function() {
      expect(new Range("a", "b", false, true).openUpper().tree()).to.deep.equal({
        class: "Range",
        lower: "a",
        upper: "b",
        upperOpen: true,
      });
    });
  });
})
