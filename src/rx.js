const { Observable } = require("rxjs/Observable");
require("rxjs/add/observable/empty");
require("rxjs/add/observable/from");
require("rxjs/add/operator/concat");
require("rxjs/add/operator/defaultIfEmpty");
require("rxjs/add/operator/filter");
require("rxjs/add/operator/isEmpty");
require("rxjs/add/operator/map");
require("rxjs/add/operator/merge");
require("rxjs/add/operator/mergeAll");
require("rxjs/add/operator/mergeMap");
require("rxjs/add/operator/publishReplay");
require("rxjs/add/operator/reduce");
require("rxjs/add/operator/toArray");
require("rxjs/add/operator/toPromise");
require("rxjs/add/operator/take");

module.exports = {
  Observable,
};

