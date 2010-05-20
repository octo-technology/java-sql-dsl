/**
 * Copyright (C) 2010 David Rousselie <drousselie@octo.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.octo.java.sql;

import static com.octo.java.sql.query.Query.c;
import static com.octo.java.sql.query.Query.e;
import static com.octo.java.sql.query.Query.f;
import static com.octo.java.sql.query.Query.select;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.octo.java.sql.exp.JavaSQLFunc;
import com.octo.java.sql.exp.Nullable;
import com.octo.java.sql.exp.Operator;
import com.octo.java.sql.exp.JavaSQLFunc.Evaluable;
import com.octo.java.sql.query.Query;
import com.octo.java.sql.query.QueryException;
import com.octo.java.sql.query.QueryGrammarException;
import com.octo.java.sql.query.SelectQuery;
import com.octo.java.sql.query.visitor.OracleQueryBuilder;

public class SelectQueryTest {
  @Test
  public void testShouldBuildSQLQueryWithoutWhereClause() throws QueryException {
    final SelectQuery query = select("*").from("table");

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClause() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue");

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneIsNullWhereClause()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).isNull();

    assertEquals("SELECT * FROM table WHERE (column IS NULL)", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndGivenOperator()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).op(Operator.EQ, "columnValue");

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithLimit() throws QueryException {
    final SelectQuery query = select("*").from("table").limit(10L);

    assertEquals("SELECT * FROM table LIMIT :limit1", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals(10L, query.getParams().get("limit1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithWhereClauseAndLimit()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue").limit(10L);

    assertEquals("SELECT * FROM table WHERE (column = :column1) LIMIT :limit2",
        query.toSql());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
    assertEquals(10L, query.getParams().get("limit2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereInClauseSimplified()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).in("columnValue1", "columnValue2");

    assertEquals("SELECT * FROM table WHERE (column IN (:column1,:column2))",
        query.toSql());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("column2"));
  }

  @Test(expected = QueryGrammarException.class)
  public void testShouldBuildSQLQueryWithOneWhereInClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).in((Object[]) null);

    query.toSql();
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereNotInClauseSimplified()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).notIn("columnValue1", "columnValue2");

    assertEquals(
        "SELECT * FROM table WHERE (column NOT IN (:column1,:column2))", query
            .toSql());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("column2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithNestedInExp() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")) //
        .in(select(c("columnIn")).from("tableIn").where(c("columnIn")).eq(2));

    assertEquals(
        "SELECT * FROM table WHERE (column IN ((SELECT columnIn FROM tableIn WHERE (columnIn = :columnIn1))))",
        query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals(2, query.getParams().get("columnIn1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereInClauseAndComparisonClause()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue1") //
        .and(c("otherColumn")).in("columnValue2", "columnValue3");

    assertEquals(
        "SELECT * FROM table WHERE ((column = :column1) AND (otherColumn IN (:otherColumn2,:otherColumn3)))",
        query.toSql());
    assertEquals(3, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("otherColumn2"));
    assertEquals("columnValue3", query.getParams().get("otherColumn3"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(null);

    assertEquals("SELECT * FROM table WHERE (column IS NULL)", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereEqOrIsNullClauseAndNullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eqOrIsNull(null);

    assertEquals("SELECT * FROM table WHERE (column IS NULL)", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereEqOrIsNullClauseAndNotNullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eqOrIsNull(42);

    assertEquals("SELECT * FROM table WHERE (column = :param1)", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals(42, query.getParams().get("param1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereEqClauseAndANullableNullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(new Nullable(null));

    assertEquals("SELECT * FROM table WHERE (column IS NULL)", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereEqClauseAndANullableNotNullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(new Nullable(42));

    assertEquals("SELECT * FROM table WHERE (column = :param1)", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals(42, query.getParams().get("param1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreThanOneWhereClauseSimplified()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue") //
        .and(c("otherColumn")).eq("otherColumnValue");

    assertEquals(
        "SELECT * FROM table WHERE ((column = :column1) AND (otherColumn = :otherColumn2))",
        query.toSql());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
    assertEquals("otherColumnValue", query.getParams().get("otherColumn2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreThanOneWhereClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(null) //
        .and(c("otherColumn")).eq(null) //
        .and(c("lastColumn")).eq("lastColumnValue");

    assertEquals(
        "SELECT * FROM table WHERE (((column IS NULL) AND (otherColumn IS NULL)) AND (lastColumn = :lastColumn1))",
        query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("lastColumnValue", query.getParams().get("lastColumn1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneJoinClauseSimpleOn()
      throws QueryException {
    final SelectQuery query = select("*").from("table1") //
        .innerJoin("table2").on(c("table1.column")).eq(c("table2.column"));

    assertEquals(
        "SELECT * FROM table1 INNER JOIN table2 ON (table1.column = table2.column)",
        query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneJoinClauseComplexOnAndWhereClause()
      throws QueryException {
    final SelectQuery query = select("*").from("table1") //
        .innerJoin("table2").on(e(c("table1.column")).eq(c("table2.column")) //
            .and(c("table1.otherColumn")).geq(42L)) //
        .where(c("lastColumn")).eq("test");

    assertEquals(
        "SELECT * FROM table1 INNER JOIN table2 ON ((table1.column = table2.column) AND (table1.otherColumn >= :table1.otherColumn1)) WHERE (lastColumn = :lastColumn2)",
        query.toSql());
    assertEquals(2, query.getParams().size());
    assertEquals(42L, query.getParams().get("table1.otherColumn1"));
    assertEquals("test", query.getParams().get("lastColumn2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseOrderBy()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue").orderBy("column");

    assertEquals(
        "SELECT * FROM table WHERE (column = :column1) ORDER BY column", query
            .toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndMultipleOrderBy()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue") //
        .orderBy("column1").asc() //
        .orderBy("column2").desc();

    assertEquals(
        "SELECT * FROM table WHERE (column = :column1) ORDER BY column1 ASC, column2 DESC",
        query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseNotNeededOrderBy()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq("columnValue") //
        .orderBy("columnValue", false);

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryOrderBy() throws QueryException {
    final SelectQuery query = select("*").from("table").orderBy("columnValue");

    assertEquals("SELECT * FROM table ORDER BY columnValue", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryOrderByAsc() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .orderBy("columnValue").asc();

    assertEquals("SELECT * FROM table ORDER BY columnValue ASC", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryOrderByDesc() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .orderBy("columnValue").desc();

    assertEquals("SELECT * FROM table ORDER BY columnValue DESC", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneIsNotNullWhereClause()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).isNotNull();

    assertEquals("SELECT * FROM table WHERE (column IS NOT NULL)", query
        .toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryStartWith() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).startWith("str");

    assertEquals("SELECT * FROM table WHERE (column LIKE :column1)", query
        .toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("str%", query.getParams().get("column1"));
  }

  @Test(expected = QueryGrammarException.class)
  public void testShouldBuildSQLQueryStartWithNullExp() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).startWith(null);

    query.toSql();
  }

  @Test(expected = QueryGrammarException.class)
  public void testShouldBuildSQLQueryStartWithEmptyString()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).startWith("");

    query.toSql();
  }

  @Test
  public void testShouldBuildSQLQueryWithContains() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).contains("str");

    assertEquals("SELECT * FROM table WHERE (column LIKE :column1)", query
        .toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("%str%", query.getParams().get("column1"));
  }

  @Test(expected = QueryGrammarException.class)
  public void testShouldBuildSQLQueryWithContainsWithNull()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).contains(null);

    query.toSql();
  }

  @Test(expected = QueryGrammarException.class)
  public void testShouldBuildSQLQueryWithContainsWithEmptyString()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).contains("");

    query.toSql();
  }

  @Test
  public void testShouldBuildSQLQueryWithInterlinkedCompExp()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).isNull() //
        .and(e(c("column")).eq("value").or(c("column")).eq("value"));

    assertEquals(
        "SELECT * FROM table WHERE ((column IS NULL) AND ((column = :column1) OR (column = :column2)))",
        query.toSql());
    assertEquals(2, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithBetweenSign() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).between("valueStart", "valueEnd") //
        .and(c("column")).eq("value");

    assertEquals(
        "SELECT * FROM table WHERE ((column BETWEEN :column1 AND :column2) AND (column = :column3))",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("valueStart", params.get("column1"));
    assertEquals("valueEnd", params.get("column2"));
    assertEquals("value", params.get("column3"));
  }

  @Test(expected = QueryGrammarException.class)
  public void testShouldBuildSQLQueryWithoutBetweenSignWhenValuesAreNull()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).between(null, null) //
        .and(c("column")).eq("value");

    query.toSql();
  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInColumnNames()
      throws QueryException {
    final SelectQuery query = select(f("myFunc", "param", 2)).from("table") //
        .where(c("column")).eq(42);

    assertEquals(
        "SELECT myFunc(:myFunc1,:myFunc2) FROM table WHERE (column = :column3)",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("param", params.get("myFunc1"));
    assertEquals(2, params.get("myFunc2"));
    assertEquals(42, params.get("column3"));
  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionWithAliasInColumnNames()
      throws QueryException {
    final SelectQuery query = select(f("myFunc", "param", 2).as("myAlias")) //
        .from("table") //
        .where(c("column")).eq(42);

    assertEquals(
        "SELECT myFunc(:myFunc1,:myFunc2) AS myAlias FROM table WHERE (column = :column3)",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("param", params.get("myFunc1"));
    assertEquals(2, params.get("myFunc2"));
    assertEquals(42, params.get("column3"));
  }

  @Test
  public void testShouldBuildSQLQueryWithColNameAndFunctionInColumnNames()
      throws QueryException {
    final SelectQuery query = select(c("col1"), //
        f("myFunc", c("colparam"), 2), //
        c("col3"), //
        f("mySecondFunc", "param2", 22)) //
        .from("table") //
        .where(c("column")).eq(42);

    final String sqlQuery = query.toSql();
    final Map<String, Object> params = query.getParams();

    assertEquals(4, params.size());
    assertEquals(2, params.get("myFunc1"));
    assertEquals("param2", params.get("mySecondFunc2"));
    assertEquals(22, params.get("mySecondFunc3"));
    assertEquals(42, params.get("column4"));

    assertEquals(
        "SELECT col1,myFunc(colparam,:myFunc1),col3,mySecondFunc(:mySecondFunc2,:mySecondFunc3) FROM table WHERE (column = :column4)",
        sqlQuery);

  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInWhereClause()
      throws QueryException {
    final SelectQuery query = select("*") //
        .from("table") //
        .where(f("upper", c("column"))).eq("AA");

    assertEquals("SELECT * FROM table WHERE (upper(column) = :upper1)", query
        .toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(1, params.size());
    assertEquals("AA", params.get("upper1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInEqClause()
      throws QueryException {
    final SelectQuery query = select("*") //
        .from("table") //
        .where(c("column")).eq(f("myFunc", "AA"));

    assertEquals("SELECT * FROM table WHERE (column = myFunc(:myFunc1))", query
        .toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(1, params.size());
    assertEquals("AA", params.get("myFunc1"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testShouldBuildSQLQueryWithMockFunctionInColumnNames()
      throws QueryException {
    final Evaluable<String> funcMock = createMock(JavaSQLFunc.Evaluable.class);
    expect(funcMock.eval("param", 1)).andReturn("1");
    expect(funcMock.eval("param2", 2)).andReturn("2");
    replay(funcMock);
    final OracleQueryBuilder queryBuilder = new OracleQueryBuilder();
    queryBuilder.addFunction("myFunc", funcMock);
    final SelectQuery query = select(f("myFunc", "param", 1),
        f("myFunc", "param2", 2)) //
        .from("table") //
        .where(c("column")).eq(42);

    assertEquals("SELECT 1,2 FROM table WHERE (column = :column1)", query
        .toSql(queryBuilder));
    verify(funcMock);
    final Map<String, Object> params = query.getParams();
    assertEquals(1, params.size());
    assertEquals(42, params.get("column1"));
    Query.clearVisitors();
  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInAndClause()
      throws QueryException {
    final SelectQuery query = select("*") //
        .from("table") //
        .where(c("col")).eq(42) //
        .and(f("upper", c("column"))).eq("AA");

    assertEquals(
        "SELECT * FROM table WHERE ((col = :col1) AND (upper(column) = :upper2))",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals(42, params.get("col1"));
    assertEquals("AA", params.get("upper2"));
  }

  @Test
  public void testShouldBuildSQLQueryWith1ValueAndEqualSign()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("col")).eq("val") //
        .and(c("column")).betweenOrOp(Operator.EQ, "valueStart", "valueEnd") //
        .and(c("column")).eq("value"); //

    assertEquals(
        "SELECT * FROM table WHERE (((col = :col1) AND (column = :column2)) AND (column = :column3))",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("val", params.get("col1"));
    assertEquals("valueStart", params.get("column2"));
    assertEquals("value", params.get("column3"));
  }

  @Test
  public void testShouldBuildSQLQueryWith2ValueAndBetweenSign()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("col")).eq("val") //
        .and(c("column")).betweenOrOp(Operator.BTW, "valueStart", "valueEnd") //
        .and(c("column")).eq("value");

    assertEquals(
        "SELECT * FROM table WHERE (((col = :col1) AND (column BETWEEN :column2 AND :column3)) AND (column = :column4))",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(4, params.size());
    assertEquals("val", params.get("col1"));
    assertEquals("valueStart", params.get("column2"));
    assertEquals("valueEnd", params.get("column3"));
    assertEquals("value", params.get("column4"));
  }

  @Test
  public void testShouldBuildSQLQueryWithUnionSelect() throws QueryException {
    final SelectQuery query = select("*")//
        .from("table") //
        .where(c("col")).eq("val") //
        .union(//
            select(c("col1"), c("NULL col2")) //
                .from("otherTable") //
                .where(c("col1")).eq("val1") //
        );

    assertEquals(
        "SELECT * FROM table WHERE (col = :col1) UNION SELECT col1,NULL col2 FROM otherTable WHERE (col1 = :col12)",
        query.toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals("val1", params.get("col12"));
    assertEquals("val", params.get("col1"));
  }
}
