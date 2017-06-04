# fluentquery

This is a structured query language (like SQL) for IndexedDB. It is embedded in JavaScript. fluentquery is _not_ an ORM.

Features:
* Schemaless / NoSQL
* Works with any IndexedDB database
* Select from both IndexedDB object stores and JavaScript arrays
* Inner joins, outer joins, anti joins, full joins and self joins
* Filter by where predicate
* Order by multiple keys
* Group by with aggregates such as sum, count, min, max, etc
* Query planner automatically chooses index based on where and join predicates
* Where and join predicates expressed as JavaScript expressions
* Insert, update and upsert rows in IndexedDB object store

It's still a work in progress but everything in the feature list is working to a degree.

## Example

```js
// Optimize query.
let query = select `{name: thing.name}`
             .from ({thing})
         .fullJoin ({type})
               .on `thing.type_id === type.id`

// Invoke query.
query().forEach(row => {
  // Do something with each row.
});
```
