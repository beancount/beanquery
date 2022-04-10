(Old notes from beancount/TODO file. Copied on {2022-04-10} by @blais.)

# Shell and Query Language

  When I implemented the SQL shell, it was intended as an experiment, and as
  such I did not bother implementing a racial set of unit tests for it, the goal
  was to move fast. It has been an ongoing experiment for a while and now I've
  gathered enough user requirements, and I've had enough new ideas about how it
  could be improve that I should move it to the next stage.

  In particular, I really want to abstract its workings away from Beancount. The
  following would allow the shell to move out of Beancount entirely, in theory:

  1. Define an abstract table source (named columns of typed data, like an R
     DataFrame),
  2. Supporting custom data types (for Beancount's Amount, Position and
     Inventory types),
  3. Supporting repeated fields (for tags and links), and
  4. Supporting structured fields, deal with flattening properly.
  5. Supporting the calling of arbitrary Python functions from the shell.
  6. bean-query's FROM clause could move to a function, something like this:

        FROM BEANCOUNT("/home/blais/ledger.beancount", postings, close=1)

  The current functionality that the FROM clause offers can be replaced by
  providing functions for the WHERE clause to use. Basically all we need is a
  single joined table of Transaction and Postings with suitable functions.

  - Implement a new syntax to abstract away from Beancount. Output the results
    of converting the SQL query to a protobuf to instill a tighter definition
    which guarantees types. (Add the protobuf dependency.)

  - I need to implement a full battery of unit tests for the functions provided
    in the shell's environment. I've neglected to do this. Do this now.

  - Convert the environment functions from classes to just regular function
    objects, taking advantage of Python3's ability to attach datatypes onto the
    arguments and return value. This should clean up the b.q.query_env code a bit.

  - Should we validate that the query is legit before we even run it? I think
    so. Compile immediately after parsing instead of loading the Beancount file
    first. This will make failing queries return immediately, a better
    experience.

  - Wouldn't it be nice if running a bean-query could automatically upload to a
    new Google Doc and bring up the web browser interface? Do this.

  - Make BALANCES command support a WHERE clause. It?s dumb not to. {balance-where}

  - Make auto-group work. {autogroup}

  - When you run a query like this:

      "select ..., position, balance where ... order by date desc"

    The balances appear in the wrong order! Compute the balances after
    reordering.

## Query Language
http://furius.ca/beancount/doc/proposal-query

  - Add dot-syntax to be able to run inequalities against the balance, e.g.
    balance.number < 1000 USD, or parse amounts, units(balance) < 1000 USD.
    Some users have inferred that this would work, so it's probably intuitive
    to others too.

  - Create tests for all the realistic test cases
    Use cases:

     # FIXME: About balance reports, what is the recommended way to remove empty
     # balances? e.g. on a balance sheet when using the CLEAR option.

     # holdings --by currency:
     #   SELECT currency, sum(change)
     #   GROUP BY currency

     # holdings --by account
     #   SELECT account, sum(change)
     #   GROUP BY account

     # networth,equity:
     #   SELECT convert(sum(change), 'USD')
     #   SELECT convert(sum(change), 'CAD')

     # prices:
     #   SELECT date, currency, cost
     #   WHERE type = 'Price'

     # all_prices:
     #   PRINT
     #   WHERE type = 'Price'

     # check,validate:
     #   CHECK

     # errors:
     #   ERRORS

     # current_events,latest_events:
     #   SELECT date, location, narration
     #   WHERE type = 'Event'

     # events:
     #   SELECT location, narration
     #   WHERE type = 'Event'

     # activity,updated:
     #   SELECT account, LATEST(date)

     # stats-types:
     #   SELECT DISTINCT COUNT(type)
     #   SELECT COUNT(DISTINCT type) -- unsure

     # stats-directives:
     #   SELECT COUNT(id)

     # stats-entries:
     #   SELECT COUNT(id) WHERE type = 'Transaction'

     # stats-postings:
     #   SELECT COUNT(*)

     # SELECT
     #   root_account, AVG(balance)
     # FROM (
     #   SELECT
     #     MAXDEPTH(account, 2) as root_account
     #     MONTH(date) as month,
     #     SUM(change) as balance
     #   WHERE date > 2014-01-01
     #   GROUP BY root_account, month
     # )
     # GROUP BY root_account


     # Look at 401k
     # select account, sum(units(change)) where account ~ '2014.*401k' group by 1 order by 1;


     # FIXME: from mailing-list:
     # SELECT account, payee, sum(change)
     # WHERE account ~ "Payable" OR account ~ "Receivable" GROUP BY 1, 2;


     # FIXME: To render holdings at "average cost", e.g. when aggregating by account,
     # you could provide an "AVERAGE(Inventory)" function that merges an inventory's
     # lots in the same way that the holdings merge right now. THIS is how to replace
     # and remove all holdings support.



  - Use the display_context in the BQL rendering routines instead of using the
    display precision mode in the displayed numbers only.


  - This should fail (it doesn't):

       SELECT DISTINCT account  GROUP BY account, account_sortkey(account) ORDER BY 2;

    I think you need to apply the ORDER-BY separately, and be able to ORDER-BY
    aggregate values.


  - The OPEN ON and CLOSE ON syntaxes get on my nerves. I need something
    simpler, maybe even something simpler for "just this year". Maybe an
    auto-open at the first transaction that occurs after filtering, something
    like this:

       FROM  year = 2014  CLAMPED

    where CLAMPED means (open + close + clear) operations.


  - Add tests for all environment functions

  - Optional: Support a 'batch mode' format to process multiple statements at
    once, reading the input files only once (needs support for redirection of
    output to files).

  - Write a documentation for the query language.


  - In docs: explain four ways to "get data out": bean-web, bean-report,
    bean-query, write script.


  - Create a setvar for style (boxed, spaced, etc.)


  - Rename 'change' column to 'position', and support dotted attribute name
    syntax. It should map onto the Python syntax one-to-one.


  - Compute the special 'balance' row and produce journals with it.


  - Cache .format methods in renderers, they may be caching the formatting
    themselves. Time the difference, see if it matters, look at CPython
    implementation to find out.

  - The current number formatting code truncates numbers longer than the mode
    and should be rounding it. Make it round.

  - Another problem is that although the mode of the precision could be
    selected to be 2, if other currencies have a higher maximum, numbers with
    greater precision than that will render to more digits. This is not nice.

  - The insertion of unrealized value in this test query is the reason we have
    14 digits of precision; this is not right, the unrealized entries should be
    generated with less precision, should be quantized to the mode of the
    precisions in the input file itself:

       select account, sum(units(change)) from close on 2015-01-01   where account ~ 'ameritra'   group by 1 order by 1;


    Time to write test for this, for the mode rounding.


  - Convert the amount renderer to use the display-context.


  - Render with custom routine, not beancount.reports.table

    * Find a way to pipe into treeify
    * Deal with rendering on multiple lines, e.g., for inventories with multiple positions


  - Implement set variables for format and verbosity and display precision and what-not



  - Support matching on other than Transactions instances.

  - You could apply an early limit only if sorting is not requested, stopping
    after the limit number of rows.

  - Implement and support the ResultSetEnvironment for nested select quereis.
    (Actually allow evaluating the SQL against generic rows of datasets.)

  - New columns and functions:
    * Add date() function to create dates from a dateutil string
    * Support simple mathematical operations, +, - , /.
    * Implement set operations, "in" for sets
    * Implement globbing matches




  - Flatten should parse closer to distinct keyword, as in SELECT FLATTEN ...

  - Maybe add format keyword followed by the desired format instead of a set var
    (or add both)

  - Redirecting output should be done with > at the end of a statement

  - "types ..." : print the inferred types of a statement, the targets, or maybe
    that's just part of EXPLAIN? DESCRIBE? Describe prints all the columns and
    functions in each environments? Or is it HELP?

  - BALANCES should use and translate operating currencies to their own column,
    and it should just work automatically. It should pull the list of operating
    currencies and generate an appropriate list of SELECT targets.

  - Create an "AROUND(date, [numdays])" function that matches dates some number
    of days before or after. We should be able to use this to view transactions
    occurring near another transaction.

  - This causes an ugly error message:
    beancount> print from has_account ~ 'Rent';

  - That's weird, why didn't those get merged together, investigate:

     beancount> select cost_currency, sum(cost(change)) where account ~
     'assets.*inv' group by 1 ;
     ,-----+-----------------------------------.
     +-----+-----------------------------------+
     | CAD | XXXXX.XXXXXXX0000000000000000 CAD |
     |     | XXXXX.XXXXXXX0000000000000000 CAD |
     | USD |                                   |
     `-----+-----------------------------------'

    This is probably due to lot-dates not being rendered.

  - You need to support "COUNT(*)", it's too common. r.Count(r.Wildcard()).

  - The shell should have a method for rendering the context before and after a
    particular transcation, and that transaction as well, in the middle. This
    should replace the "bean-doctor context" command.

  - As a special feature, add an option to support automatic aggregations,
    either implicitly with a set-var, or with the inclusion and support of
    "GROUP BY *", or maybe "GROUP BY NATURAL" which is less misleading than
    "GROUP BY *". Or perhaps just "GROUP" with the "BY ..." bit being optional.
    I like that.

    Although MySQL treats it differently: "If you use a group function in a
    statement containing no GROUP BY clause, it is equivalent to grouping on all
    rows. For more information, see Section 12.17.3, ?MySQL Handling of GROUP
    BY?."



  - For the precision, create some sort of context object that will provide
    the precision to render any number by, indexed by commodity. This should be
    accumulated during rendering and then used for rendering.

  - Provide an option to split apart the commodity and the cost commodity
    into their own columns. This generic object should be working for text, and
    then could be simply reused by the CSV routines.

  - Add EXPLODE keyword to parser in order to allow the breaking out of the
    various columns of an Inventory or Position. This design is a good balance of
    being explicit and succint at the same time. The term 'explode' explains well
    what is meant to happen.

       SELECT account, EXPLODE sum(change) ...

    will result in columns:

        account, change_number, change_currency, change_cost_number, change_cost_currency, change_lot_date, change_lot_label



  - Idea: support entry.<field> in the targets and where clauses. This would
    remove the need to have duplicated columns, would make the language simpler
    and more intuitive.


  - Idea: Another output data format for the reports/query language could be
    parseable Python format.



  - (query syntax) It *would* make sense to use full SQL for this, even if the
    aggregation method is an inventory.

      targets: units, cost, market, lots
      data-source: balances, journal, holdings
      restricts: ... all the conditions that match transactions, with = ...
      aggregations: by currency, by day, by month, by account (regexp), etc.
      other: filter display, pivot table (for by-month reports), max depth

    You would render these as a table.

  - Implement a "reload" command to avoid having to leave the shell after the
    file changes. Maybe we should even have an "autoreload" feature that just
    kicks in before a query, like the web interface.


  - Move bean-example to being just a doctor subcommand; we really don't need to
    make that a first-class thing.

  - Support constants for flags, e.g. flags.conversion is equivalent to 'C'.
    Add those to our existing unit tests.

  - Create test cases for all query_env, including evaluation. The list of tests
    is currently not exhaustive.


  - Operating currencies getting pulled out are necessary... maybe do this in
    the translation?

  - Support COUNT(), and COUNT(*), for this question on the ledger-cli list:
    https://groups.google.com/d/msg/ledger-cli/4d9ZYVLnCGQ/ZyAqwZE-TBoJ
    Try to reproduce this specific use case.

  - Generate balance auto-columns by referencing existing columns in the query,
    not as a hard-coded column. Something like this:

      SELECT position, SUM(convert(position, "USD", date)) as usd_amount, BALANCE(usd_amount) as usd_balance WHERE ...

    For a compelling example, see
    https://groups.google.com/d/msgid/beancount/20181207161308.56ivkgujculalx7g%40jirafa.cyrius.com.

## V2

  - I think we can do prety well like this:

      SELECT ... FROM transactions|postings|balance|...
      WHERE ANY(...)
            ALL(...)

    I'm not sure where OPEN CLOSE and CLEAR all fit though.

  - The table provider should support two kinds of fields: single and repeated.
    Repeated fields include Position, but also Tags. By default, rendering
    should put the entire contents in one cell/line, all only when using
    BROADCAST or FLATTEN should multiple lines be created. Maybe the Inventory
    datatype could be removed and instead be provided as a repeated field of
    Position instances.

  - The ad-hoc alignment of numbers present in the query_render.DecimalRenderer
    code should be removed, and all rendering should occur via DisplayContext.

  - Numberification should probably occur with a flag of some sort, or perhaps a
    shell variable. Not sure.

  - A better representation of a query should be produced, perhaps in a
    protobuf, with cross-reference capability and the ability to create a
    processing tree for each row.

  - Dot syntax should definitely be supported. This is how we'll get rid of the
    FROM clause.

  - Keep in mind that the booking branch might break a lot of user queries.

  - "SELECT *" should really render _all_ the possible fields, not just a
    sensible subset. This has been annoying me a lot.

  - The compiler should output Python code to evaluate and process the results,
    instead of interpreting the tree of operations.

  - In bean-sql, render out the tags to their own table and create a 1:N join
    table for them. In Beancount, provide a new table of tags. Either way. Tags
    could be their own table.

## SQL Shell (saved notes from 'shell' branch)

  - Rewrite the shell code from scratch to be independent of Beancount:

    * Compilation should generate a Python AST and that should get compiled by
      the Python compiler and executed directly.

    * Data sources should be abstracted away to provide rows of any type. This
      should include support for dotted notation. The main data source type
      should be "beancount.postings" as in

        "from beancount.postings:/path/to/filename"

    * It should be possible to provide the schema separately, in the SQL.

    * The new rendering code should use the DisplayContext. This would close the
      "display_context" branch.

    * Type checking should be implemented using type annotations, but it should
      also be implemented on the basic operators (see #6, for instance).


## SQLite3 Integration

  - Another area of shell experimentation is that I should build some way to
    provide Beancount's input as a virtual table in SQLite3, even if it's not
    possible to implement custom datatypes. I can't foresee using this myself
    but I can imagine other people getting creative with this, and Beancount
    could benefit from having the full set of SQL operations from SQLite
    available to play on its data.

    Note that this is distinct from bean-sql: bean-sql first loads the input
    into a table. What I'm thinking of instead is to create a virtual table,
    directly from the input file, without a conversion step. (Of course, if I do
    that, bean-sql's capability should be subsumed by this new tool.)

## Parser

  - Support arithmetic operations as targets, so you could SELECT 2+2, for
    example. Then add a PRICE(ccy, ccycost, date) function to pull the price at
    any date.

    * Test negative numbers
    * Test operators without spaces

    IMPORTANT: Some code has been merged for this, but numerical expressions
    like 2-3 and 2+3 don't work because the INTEGER and DECIMAL tokens include a
    potential sign. What must be done is to make the processing of those tokens
    not have a sign for the purpose of parsing expressions and make + and -
    unary expressions.

  - SQL: "IS" and "IS NOT" is not implemented.

  - Idea: The "HELP" command of the SQL shell should be made analogous to the
    schema inspection facilities of other SQL shells instead of being dedicated
    HELP commands.

  - Implement a DESCRIBE command to the SQL shell in order provide help on the
    available row commands. I think this would be a natural way to do this.

  - This query works:

      bean-query $L ' balances from flag = "!" '

    But this query fails:

      bean-query $L " balances from flag = '\!' "

    The second one needs to have the flag escaped because of bash shell
    expansion, but the problem is that the escaped backslash appears in the
    output. This is normal bash behavior, but the problem is that the user
    receives no notification of failure in this case. Beancount should detect
    that the string compared to a flag is not a single-character string and
    issue an appropriate error message for it.

  - Bug: this query fails and should not:
    "select account, sum(position) group by account order by account_sortkey(account)"

  - SQL: When not specified, ORDER BY should be set to be the same as GROUP BY
    by default. This is a sensible choice.

  - Implement implitict GROUP BY and BALANCES ... WHERE syntax

  - The 'balances' report should also support a WHERE clause as a nice
    shorthand. I would use that all the time myself if I could.


### Implement Table Joins

  - Write the multi-year report and share on the list at
    https://groups.google.com/d/msg/ledger-cli/XNIK853ExNc/CWxSPa-5INMJ

  - Write a utility script that merges multiple reports with a leftmost column
    of account names into a single report with multiple columns.

       SELECT account, bal1, bal2 FROM
         (SELECT account, sum(cost(position)) as bal1
          FROM CLOSE ON 2014-01-01 CLEAR)
         JOIN
         (SELECT account, sum(cost(position)) as bal2
          FROM CLOSE ON 2015-01-01 CLEAR)
         ON account;

## query_env

  - There's a bug in the MIN() function, it fails, try this:
      select min(balance) from open on 2015-01-01 close  on 2016-01-01  where account = ...

  - Rename ACCOUNT_SORTKEY(), it's a terrible name. Name this REPORD() for
    "report order".

  - Provide a SUBSTR() or SUB() function for the SQL script. Find out what the
    SQL standard is and implement that. MAXWIDTH() just isn't too great a name.
    TRIM() might have been a better name.

## query_eval

  - Another problem with queries is that sales cause very large unrealistic
    changes because each posting affects its account separately. Look at this
    query, for example, where we are trying to obtain the maximum balance during
    the year:

      select date, description, convert(balance, 'CAD', date)  from open on 2015-01-01
      close  on 2016-01-01  where parent(account) = .../'rrsp'

    We need to find a way to report the balance only after all the postings of a
    particular transaction are applied. I'm not sure how to handle this well yet.

  - query: Provide a column for the "other accounts" of a selected posting's
    transactions, so you can select on that. Selecting a transaction should be
    migrated from the "FROM" syntax to the "WHERE" syntax as if a joined table,
    with suitable support for ANY and other membership operatos.


## Support Negative filtering by default

  - Idea: For "virtual postings", you could mark certain tags to be excluded by
    default, to be included only explicitly. e.g. #virtual tag would have to be
    brought in by selecting it via "tag:virtual". Maybe a different prefix would
    be used to distinguish them, e.g. #virtual and %virtual, or #virtual and
    -#virtual; something like that.

## query_render

  - Journal rendering: add terminal colors (easy).

## Misc Grab Bag of Ideas

  - The bean-query --numberify option does not split columns when the output is
    text (default). Only for csv. This is inconsistent.

  - "numberify" is only made available from run_query(). This is insufficient;
    it needs be made available through the SQL shell output (as an option) and
    even from the Google sheets uploader.

    In addition, this ought to work as a standalone tool, whereby a CSV file's
    column types (including Beancount-specific ones like Amount, Position and
    Inventory) should be inferred automatically, and the transformations can be
    applied to them.

    Make a nice library, plug this in many places, and make a standalone script
    as well.

  - Build a script that can run the reports found in Query and output them
    either into files or all at once. This is probably TBD as part of bean-query
    itself, with some options.

  - Write unit tests for query_env and the rest of the package (important, bugs
    are being found by others).

  - Convert the query rendering routines to use the DisplayContext
    https://bitbucket.org/blais/beancount/issues/105/context-the-query-rendering-routines-to
