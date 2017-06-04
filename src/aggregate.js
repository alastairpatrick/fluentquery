"use strict";

const { cmp } = require("./idbbase");

const has = Object.prototype.hasOwnProperty;

const avg = (state, v) => {
  if (state === undefined) {
    state = {
      total: 0,
      count: 0,
      value: undefined,
    };
  }

  if (v === null || v === undefined)
    return state;

  state.total += v;
  state.count++;
  state.value = state.total / state.count;
  return state;
}
avg.isAggregate = true;

const count = (state, v) => {
  if (state === undefined) {
    state = {
      value: 0,
    };
  }

  if (v === null || v === undefined)
    return state;

  state.value++;
  return state;
}
count.isAggregate = true;

const max = (state, v) => {
  if (state === undefined) {
    state = {
      value: undefined,
    };
  }

  if (v === null || v === undefined)
    return state;

  if (state.value === undefined || cmp(state.value, v) < 0)
    state.value = v;

  return state;
}
max.isAggregate = true;

const min = (state, v) => {
  if (state === undefined) {
    state = {
      value: undefined,
    };
  }

  if (v === null || v === undefined)
    return state;

  if (state.value === undefined || cmp(state.value, v) > 0)
    state.value = v;

  return state;
}
min.isAggregate = true;

const sum = (state, v) => {
  if (state === undefined) {
    state = {
      value: 0,
    };
  }

  if (v === null || v === undefined)
    return state;

  state.value += v;
  return state;
}
sum.isAggregate = true;

const stdAggregates = {
  avg,
  count,
  max,
  min,
  sum,
}

module.exports = {
  stdAggregates,
};
