Java SQL DSL
============
java-sql-dsl is a small DSL to generate SQL queries in Java. It will
be able to remove unecessary "where clauses" when given value is
null. It then avoids to add if conditions to build the SQL query.

For example, a typical Select query will look like this :
        select("*").from("client") //
        .where(c("firstname")).eq(criteria.getClientFirstname()) //
        .and(c("lastname")).eq(criteria.getClientLastname())
        ...

This query will filter result on firstname only if the criteria has a
clientFirstname which is not null.
Thus, it is possible to write only one "readable" query for every
combinations of submitted criteria.

Usage
-----
1. Instanciating queries

- A SelectQuery is instanciated with the static method
  ``Query.select(columns/functions/"*")``.
- An UpdateQuery is instanciated with the static method
  ``Query.update(table)``.
- An InsertQuery is instanciated with the static method
  ``Query.insertInto(table)``.
- A DeleteQuery is instanciated with the static method
  ``Query.deleteFrom(table)``.

2. Producing SQL

To get the SQL query as a string, just call the ``toSql()`` method
when the query has been built.
To get a parameter ``Map<String, Object>``, call the ``getParams()``
method.

A typical usage with a Spring ``SimpleJDBCTemplate`` might look like
this :
        getNamedParameterJdbcTemplate().query(query.toSql(),
        query.getParams(), myRowMapper);

4. Using Columns and Function

- Columns must be specified with the ``Query.c(columnName)`` static
  method.
- Functions must be specified with the ``Query.f(functionName, arg1,
  arg2, ...)`` static method.

5. Using SQL dialects

The default SQL dialect is implemented in the ``DefaultQueryBuilder``
class but other can be written by overriding the necessary ``visit()``
methods. See the ``OracleQueryBuilder`` for example.

It is then possible to replace the default query builder with :
        Query.setDefaultQueryBuilder(OracleQueryBuilder.class);

6. Optimizing requests

By default, the queries are not optimized (ie. filter clauses are not
removed if the value is null).
To optimize query, just add an instance of ``BasicQueryOptimizer`` to
the query visitors :
        Query.addVisitor(new BasicQueryOptimizer());

Additional visitors are executed just before producing the SQL query
as a string.

7. Examples

For usage examples, see the unit tests in src/main/test directory. It
will demonstrate implemented features and how to use it. The whole SQL
language is not (yet) implemented, feel free ;).
