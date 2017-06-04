# fluentquery

This is a structured query language (like SQL) for IndexedDB. It is embedded in JavaScript. fluentquery is _not_ an ORM.

Features:
* Works with any IndexedDB database
* Just IndexedDB object stores and indexes - no schema or other shenanigans layered on top
* Select from both IndexedDB object stores and JavaScript arrays
* Inner joins, outer joins, anti joins, full joins, self joins and sub-queries
* Filter and join based on JavaScript expressions
* Order by multiple keys
* Group by with aggregates such as sum, count, min, max, etc
* Query planner uses static analysis of JavaScript to determine which indexes to use
* Insert, update and upsert rows in IndexedDB object store

It's still a work in progress but everything in the feature list is working to a degree.

## Example

```js
// Plan / optimize query once.
let query = select `{name: thing.name}`
             .from ({thing})
         .fullJoin ({type})
               .on `thing.type_id == type.id`

// Invoke query many times.
query().forEach(row => {
  // Do something with each row.
});
```
