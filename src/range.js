"use strict";

const { cmp } = require("./idbbase");
const ulp = require("ulp");

const has = Object.prototype.hasOwnProperty;

const MAX_TIME = 8640000000000000;

// Returns least nextV such that cmp(nextV, v) > 0.
// A range L <= x <= U is equivalent to L <= x < nextUp(U).
const nextUp = (v) => {
  if (typeof v === "number") {
    if (v >= Infinity)
      return new Date(-MAX_TIME);
    else
      return ulp.nextUp(v);
  } else if (typeof v === "string") {
    return v + '\0';
  } else if (Array.isArray(v)) {
    return v.concat(-Infinity);
  } else {
    let time = v.getTime();
    if (time >= MAX_TIME)
      return "";
    else
      return new Date(time + 1);
  }
}

class Range {
  constructor(lower, upper, lowerOpen=false, upperOpen=false) {
    this.lower = lower;
    this.upper = upper;
    this.lowerOpen = lowerOpen;
    this.upperOpen = upperOpen;
  }

  // Here and in other Range classes, returned array is ordered list of non-overlapping ranges.
  prepare(context) {
    if (this.lower === undefined || this.upper === undefined) {
      return [this];
    } else {
      let c = cmp(this.lower, this.upper);
      if (c < 0 || (c === 0 && !this.lowerOpen && !this.upperOpen))
        return [this];
      else
        return [];
    }
  }

  includes(v) {
    if (v === undefined)
      return false;

    if (this.lower !== undefined) {
      let c = cmp(v, this.lower);
      if (c < 0)
        return false;
      if (c === 0 && this.lowerOpen)
        return false;
    }

    if (this.upper !== undefined) {
      let c = cmp(v, this.upper);
      if (c > 0)
        return false;
      if (c === 0 && this.upperOpen)
        return false;
    }

    return true;
  }

  // The conservative thing for this function to do is return false if unknown.
  isEquality() {
    return !this.lowerOpen && !this.upperOpen && cmp(this.lower, this.upper) === 0;
  }

  cmpLower(b) {
    if (this.lower === undefined) {
      return b.lower === undefined ? 0 : -1;
    } else if (b.lower === undefined) {
      return 1;
    } else {
      let c = cmp(this.lower, b.lower);
      if (c !== 0) {
        return c;
      } else if (this.lowerOpen < b.lowerOpen) {
        return -1;
      } else if (b.lowerOpen < this.lowerOpen) {
        return 1;
      } else {
        return -this.cmpUpper(b);
      }
    }
  }

  cmpUpper(b) {
    if (this.upper === undefined) {
      return b.upper === undefined ? 0 : 1;
    } else if (b.upper === undefined) {
      return -1;
    } else {
      let c = cmp(this.upper, b.upper);
      if (c !== 0) {
        return c;
      } else if (this.upperOpen < b.upperOpen) {
        return 1;
      } else if (b.upperOpen < this.upperOpen) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  // Only works if this.lower precedes b.lower.
  partialIntersects(b) {
    if (b.lower === undefined || this.upper === undefined)
      return true;

    let c = cmp(b.lower, this.upper);
    return (c < 0 || (c === 0 && !(b.lowerOpen && this.upperOpen)));
  }

  copy() {
    return new Range(this.lower, this.upper, this.lowerOpen, this.upperOpen);
  }

  openUpper() {
    if (this.upper === undefined || this.upperOpen)
      return this;
    return new Range(this.lower, nextUp(this.upper), this.lowerOpen, true);
  }

  tree() {
    let result = {
      class: this.constructor.name,
    };
    if (this.lower !== undefined)
      result.lower = this.lower;
    if (this.upper !== undefined)
      result.upper = this.upper;
    if (this.lowerOpen)
      result.lowerOpen = this.lowerOpen;
    if (this.upperOpen)
      result.upperOpen = this.upperOpen;
    return result;
  }
}

const includes = (ranges) => (v) => {
  for (let i = 0; i < ranges.length; ++i) {
    if (ranges[i].includes(v))
      return true;
  }
  return false;
}

class RangeExpression {
  constructor(lowerFn, upperFn, lowerOpen=false, upperOpen=false) {
    this.lowerFn = lowerFn;
    this.upperFn = upperFn;
    this.lowerOpen = lowerOpen;
    this.upperOpen = upperOpen;
  }

  prepare(context) {
    let tuple = context.tuple;
    return new Range(
      this.lowerFn ? this.lowerFn.call(context, tuple) : undefined,
      this.upperFn ? this.upperFn.call(context, tuple) : undefined,
      this.lowerOpen,
      this.upperOpen
    ).prepare(context);
  }

  // The conservative thing for this function to do is return false if unknown.
  isEquality() {
    return !this.lowerOpen && !this.upperOpen && this.lowerFn === this.upperFn;
  }

  tree() {
    let result = {
      class: this.constructor.name,
    };
    if (this.lowerFn !== undefined)
      result.lower = this.lowerFn.source || this.lowerFn.toString();
    if (this.upperFn !== undefined)
      result.upper = this.upperFn.source || this.upperFn.toString();
    if (this.lowerOpen)
      result.lowerOpen = this.lowerOpen;
    if (this.upperOpen)
      result.upperOpen = this.upperOpen;
    return result;
  }
}

class RangeUnion {
  constructor(left, right) {
    this.left = left;
    this.right = right;
  }

  prepare(context) {
    let left = this.left.prepare(context);
    let right = this.right.prepare(context);

    let input = [];
    while (left.length && right.length) {
      if (left[0].cmpLower(right[0]) < 0)
        input.push(left.shift())
      else
        input.push(right.shift());
    }
    input = input.concat(left, right);

    let result = [];
    while (input.length) {
      let at = input.shift().copy();

      while (input.length && at.partialIntersects(input[0])) {
        if (at.cmpUpper(input[0]) < 0) {
          at.upper = input[0].upper;
          at.upperOpen = input[0].upperOpen;
        }

        input.shift();
      }

      result.push(at);
    }

    return result;
  } 

  // The conservative thing for this function to do is return false if unknown.
  isEquality() {
    return false;
  }

  tree() {
    return {
      class: this.constructor.name,
      left: this.left.tree(),
      right: this.right.tree(),
    };
  }
}

class RangeIntersection {
  constructor(left, right) {
    this.left = left;
    this.right = right;
  }

  prepare(context) {
    let left = this.left.prepare(context);
    let right = this.right.prepare(context);

    let result = [];
    while (left.length && right.length) {
      if (left[0].cmpLower(right[0]) > 0) {
        let temp = left;
        left = right;
        right = temp;
      }
      let at = left.shift();

      while (right.length && at.partialIntersects(right[0])) {
        let inter = right.shift().copy();
        if (inter.cmpUpper(at) > 0) {
          inter.upper = at.upper;
          inter.upperOpen = at.upperOpen;
        }
        result.push(inter);
      }
    }

    return result;
  }

  // The conservative thing for this function to do is return false if unknown.
  isEquality() {
    return this.left.isEquality() || this.right.isEquality();
  }

  tree() {
    return {
      class: this.constructor.name,
      left: this.left.tree(),
      right: this.right.tree(),
    };
  }
}

const compositeRange = (equals, range) => {
  let lower = equals, upper = equals;
  let lowerOpen = range.lowerOpen, upperOpen = range.upperOpen;

  if (range.lower !== undefined)
    lower = lower.concat([range.lower]);

  if (range.upper !== undefined) {
    upper = upper.concat([range.upper]);
    if (!range.upperOpen) {
      upper[upper.length - 1] = nextUp(upper[upper.length - 1]);
      upperOpen = true;
    }
  } else {
    if (upper.length === 0) {
      upper = undefined;
    } else {
      upper = upper.slice(0);
      upper[upper.length - 1] = nextUp(upper[upper.length - 1]);
      upperOpen = true;
    }
  }
    
  return new Range(lower, upper, lowerOpen, upperOpen);
}

module.exports = {
  Range,
  RangeExpression,
  RangeIntersection,
  RangeUnion,
  compositeRange,
  includes,
  nextUp,
};
