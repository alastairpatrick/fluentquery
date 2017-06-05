# fluentquery

This is a structured query language (like SQL) for IndexedDB, embedded in JavaScript.

Features:
* Works with any IndexedDB database
* Just IndexedDB object stores and indexes - no schema or other shenanigans layered on top
* Select from both IndexedDB object stores and JavaScript arrays
* Inner joins, outer joins, anti joins, full joins, self joins and sub-queries
* Filter and join based on JavaScript expressions
* Order by multiple keys
* Group by with aggregates such as sum, count, min, max, etc
* Query planner uses static analysis of JavaScript to determine which indexes to use
* Insert, update and upsert rows in IndexedDB object stores
* Also works on [fakeIndexedDB](https://github.com/dumbmatter/fakeIndexedDB

It's still a work in progress but everything in the feature list is working to a degree.

fluentquery is not an ORM.

## Example

```js
// Plan / optimize query once.
let query = select `{ name: employee.last_name + ", " + employee.first_name,
                      order_id: order.id,
                    }`
             .from ({employee})
         .fullJoin ({order})
               .on `order.employee_id == employee.id`
          .orderBy `employee.last_name` .asc

// Invoke query many times.
query().forEach(row => {
  // Do something with rows as they become available.
}).then(() => {
  // Do something on completion.
});

// or...
query().then(rows => {
  // Do something with all rows at once.
});

```
