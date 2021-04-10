# Notes on Future Bean-query

Overall the goal is to make `beanquery` a generic tool



## Goals

* **Generic Inputs** Turn the input into generic table providers, so that this
  can work on any tabular data. The schema for the table input potentially would
  reuse the protobuf Descriptor protos. Convert the `FROM` clause to a more
  generic one that can import from various tables extracted from a Beancount
  repository (e.g., `FROM beancount(filename, 'postings')`, `FROM
  beancount(filename, 'prices')`), etc. The FROM clause can then read CSV files
  and directory listings, and other sources.

* **Data Types** Make "hooks" available so that one can insert custom functions
  and data types, and make all Beancount custom types (`Amount`, `Position`,
  `Inventory`) use these hooks to make the generic tool work on Beancount tables
  of postings.

* **Tests.** Review the entire codebase and write unit tests for everything.
  This hasn't been tested nearly enough. Add type annotations everywhere.

* **Precision.** Redesign the rendering of numbers based on precision. There
  needs to be an object in here that stores the precision of each number based
  on context, i.e., the (base, quote) currencies, and this needs to be
  initializable from Beancount's object (again, decoupled, but compatible). It
  should be possible to run beanquery outside of the context of Beancount
  entirely (all Beancount support ideally would be added from hooks).

* **Functions.** Review all functions and their names. Functions should be
  converted from classes to just regular functions with type annotations and
  using inspection to do type checking. Aggregator functions should acquire a
  new interface.


# Current Code and Notes

(10 minute brain dump.)

shell.py
shell_test.py

* This is the top-level command-line script that also supports an interactive
  mode and readline and history.

query.py
query_test.py

* That's a simple API to run commands from a Python program. Ideally this would
  be somehow folded elsewhere and shell.py above would call the same code.
  Doesn't matter that much.

query_parser.py
query_parser_test.py

* This is the SQL parser. It uses PLY and it does *not* need to be fast (PLY is
  just fine for this, no need to introduce C code).

* It produces nested namedtuples to describe the query (this could be formalized
  a bit more (proto?) but it works nicely now). Also, the data structure which
  describes the query could be made much better, so that in theory another
  simpler query language could be implemented on top and generate the same query
  data structure, for example, a simplified ands-of-ors like in Gmail.

* The HAVING clause is parsed but was never implemented. It should be either
  removed or implemented.

query_compile.py
query_compile_test.py

* This code translates the namedtuples from the parser into something that's
  ready for execution, creating evaluator objects and aggregators (with state,
  e.g. for GROUP BY operations). It resolves column names and column indices.
* Ideally this code would be improved to implement type checking.

query_execute.py
query_execute_test.py

* This code basically iterates (loops) over the input rows and executes the
  evaluators on each of the rows. It literally is a Python loop. It could be
  optimized, but I don't think this is very important for this project. (The
  strength of beanquery is not performance, it's customizability and the ability
  to insert it into any pure-Python program to have some SQL in there.)

query_env.py
query_env_test.py

* This code defines all the functions available to call from the language. There
  are different sets of functions available from the `FROM` and `WHERE` clauses,
  and function name aliases.

* I let this evolve as it went, not taking too much care for naming the
  functions consistently. It's a bit of a mess and could certainly use a nice
  holistic review of all the naming.

* I'd like to replace all (most?) of those classes by simple functions with type
  annotations, and have code that inspects the type annotations to infer the in
  and out data types. Then I'd like to make it easier for people to add
  functions.

* All functions here that are operating on Beancount-specific types ideally
  should be seggregated to another file and inserted via hooks, so that a user
  that just needs to process a CSV file (not Beancount context) can stil use the
  tool. Basically this tool should transcent Beancount and be usable on any
  tabular data that we can create a source for.

query_render.py
query_render_test.py

* This code renders each of the data types to strings to be written out.

* In particular, the formatting of numbers is problematic here and has never
  been reconciled with the rest of Beancount. Ideally it should follow
  Beancount's pattern and allow for a "display context" object of sort to be
  able to provide formatting strings for each numbers, and the context should be
  the (base, quote) currencies in effect, for instance, when rendering an
  `Amount` in `USD` that is the price of some `JPY` units, the number of digits
  used may be different than that which is used to price shares of `HOOL`.
  Something like that. The display context should be independent though,
  ultimate we want this project to have minimal dependencies on Beancount (well,
  at least, optional ones), but it should be initializable from whatever the
  future of that display context object is in Beancount.

numberify.py
numberify_test.py

* That's code to handle the Amount data type (which is a pair of number and a
  currency) to split it into two columns so that when loading into a spreadsheet
  that numbers can be processed (in their own column). Currently this is
  triggered by a flag (-m) but if you can find a better way to express this
  (with some SQL syntax extension?) that might be even better.
