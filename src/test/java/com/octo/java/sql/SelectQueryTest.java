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

import static com.octo.java.sql.Query.col;
import static com.octo.java.sql.Query.func;
import static com.octo.java.sql.Query.select;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.octo.java.sql.QueryPart.Operator;

public class SelectQueryTest {
  @Test
  public void testShouldBuildSQLQueryWithoutWhereClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table");

    assertEquals("SELECT * FROM table", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").eq("columnValue", false));

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query
        .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneIsNullWhereClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").isNull());

    assertEquals("SELECT * FROM table WHERE (column IS NULL)", query
        .buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseSimplified()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").eq(
        "columnValue", false);

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query
        .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndGivenOperator()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").op(
        Operator.EQ, "columnValue");

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query
        .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithLimit() throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").limit(10L);

    assertEquals("SELECT * FROM (SELECT * FROM table) WHERE (rownum<=:limit)",
        query.buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals(10L, query.getParams().get("limit"));
  }

  @Test
  public void testShouldBuildSQLQueryWithWhereClauseAndLimit()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").eq(
        "columnValue").limit(10L);

    assertEquals(
        "SELECT * FROM (SELECT * FROM table WHERE (column = :column1)) WHERE (rownum<=:limit)",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
    assertEquals(10L, query.getParams().get("limit"));
  }

  @Test
  public void testShouldBuildSQLQueryWithWhere2ClauseAndLimit()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").eq("columnValue")).limit(10L);

    assertEquals(
        "SELECT * FROM (SELECT * FROM table WHERE (column = :column1)) WHERE (rownum<=:limit)",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
    assertEquals(10L, query.getParams().get("limit"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereInClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").in("columnValue1", "columnValue2"));

    assertEquals("SELECT * FROM table WHERE (column IN (:column1,:column2))",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("column2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereInClauseSimplified()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").in(
        "columnValue1", "columnValue2");

    assertEquals("SELECT * FROM table WHERE (column IN (:column1,:column2))",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("column2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereNotInClauseSimplified()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").notIn(
        "columnValue1", "columnValue2");

    assertEquals(
        "SELECT * FROM table WHERE (column NOT IN (:column1,:column2))", query
            .buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("column2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithNestedInExp()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").in(
        select("columnIn").from("tableIn").where("columnIn").eq(2));

    assertEquals(
        "SELECT * FROM table WHERE (column IN (SELECT columnIn FROM tableIn WHERE (columnIn = :columnIn1)))",
        query.buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals(2, query.getParams().get("columnIn1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereInClauseAndComparisonClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").eq("columnValue1", false)
            .and("otherColumn").in("columnValue2", "columnValue3"));

    assertEquals(
        "SELECT * FROM table WHERE ((column = :column1) AND (otherColumn IN (:otherColumn2,:otherColumn3)))",
        query.buildSQLQuery());
    assertEquals(3, query.getParams().size());
    assertEquals("columnValue1", query.getParams().get("column1"));
    assertEquals("columnValue2", query.getParams().get("otherColumn2"));
    assertEquals("columnValue3", query.getParams().get("otherColumn3"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndANullValue()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").eq(null, false));

    assertEquals("SELECT * FROM table", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreThanOneWhereClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").eq("columnValue", false).and("otherColumn")
            .eq("otherColumnValue", false));

    assertEquals(
        "SELECT * FROM table WHERE ((column = :column1) AND (otherColumn = :otherColumn2))",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
    assertEquals("otherColumnValue", query.getParams().get("otherColumn2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreThanOneWhereClauseSimplified()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").eq(
        "columnValue").and("otherColumn").eq("otherColumnValue");

    assertEquals(
        "SELECT * FROM table WHERE ((column = :column1) AND (otherColumn = :otherColumn2))",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
    assertEquals("otherColumnValue", query.getParams().get("otherColumn2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreThanOneWhereClauseAndANullValue()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").eq(null, false).and("otherColumn").eq(null,
            false).and("lastColumn").eq("lastColumnValue", false));

    assertEquals("SELECT * FROM table WHERE ((lastColumn = :lastColumn1))",
        query.buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("lastColumnValue", query.getParams().get("lastColumn1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneJoinClauseSimpleOn()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table1")
        .leftInnerJoin("table2").on("table1.column").eq("table2.column");

    assertEquals(
        "SELECT * FROM table1 LEFT INNER JOIN table2 ON (table1.column = table2.column)",
        query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneJoinClauseComplexOnAndWhereClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table1")
        .leftInnerJoin("table2").on(
            new ComparisonExp("table1.column").eq("table2.column", true).and(
                "table1.otherColumn").geq(42L, false)).where(
            new ComparisonExp("lastColumn").eq("test", false));

    assertEquals(
        "SELECT * FROM table1 LEFT INNER JOIN table2 ON ((table1.column = table2.column) AND (table1.otherColumn >= :table1.otherColumn1)) WHERE (lastColumn = :lastColumn2)",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
    assertEquals(42L, query.getParams().get("table1.otherColumn1"));
    assertEquals("test", query.getParams().get("lastColumn2"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseOrderBy()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").eq(
        "columnValue", false).orderBy("column");

    assertEquals(
        "SELECT * FROM table WHERE (column = :column1) ORDER BY column", query
            .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndMultipleOrderBy()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").eq(
        "columnValue", false).orderBy("column1").asc().orderBy("column2")
        .desc();

    assertEquals(
        "SELECT * FROM table WHERE (column = :column1) ORDER BY column1 ASC, column2 DESC",
        query.buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseNotNeededOrderBy()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column").eq(
        "columnValue", false).orderBy("columnValue", false);

    assertEquals("SELECT * FROM table WHERE (column = :column1)", query
        .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("columnValue", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryOrderBy() throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").orderBy("columnValue");

    assertEquals("SELECT * FROM table ORDER BY columnValue", query
        .buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryOrderByAsc() throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").orderBy("columnValue")
        .asc();

    assertEquals("SELECT * FROM table ORDER BY columnValue ASC", query
        .buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryOrderByDesc() throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").orderBy("columnValue")
        .desc();

    assertEquals("SELECT * FROM table ORDER BY columnValue DESC", query
        .buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneIsNotNullWhereClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").isNotNull());

    assertEquals("SELECT * FROM table WHERE (column IS NOT NULL)", query
        .buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryStartWith() throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").startWith("toto"));

    assertEquals("SELECT * FROM table WHERE (column LIKE :column1)", query
        .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("toto%", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryStartWithNullExp()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").startWith(null));

    assertEquals("SELECT * FROM table", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryStartWithEmptyString()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").startWith(""));

    assertEquals("SELECT * FROM table", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithContains()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column")
        .contains("toto");

    assertEquals("SELECT * FROM table WHERE (column LIKE :column1)", query
        .buildSQLQuery());
    assertEquals(1, query.getParams().size());
    assertEquals("%toto%", query.getParams().get("column1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithContainsWithNull()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column")
        .contains(null);

    assertEquals("SELECT * FROM table", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithContainsWithEmptyString()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column")
        .contains("");

    assertEquals("SELECT * FROM table", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithInterlinkedCompExp()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new ComparisonExp("column").isNull().and(
            new ComparisonExp("column").eq("value").or("column").eq("value")));

    assertEquals(
        "SELECT * FROM table WHERE ((column IS NULL) AND ((column = :column1) OR (column = :column2)))",
        query.buildSQLQuery());
    assertEquals(2, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithBetweenExp()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where(
        new BetweenExp("column", "valueStart", "valueEnd").and("colum").eq(
            "value"));

    assertEquals(
        "SELECT * FROM table WHERE ((column BETWEEN :column1 and :column2) AND (colum = :colum3))",
        query.buildSQLQuery());
    assertEquals(3, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithBetweenSign()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("column")
        .between("valueStart", "valueEnd").and("column").eq("value");

    assertEquals(
        "SELECT * FROM table WHERE ((column BETWEEN :column1 and :column2) AND (column = :column3))",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("valueStart", params.get("column1"));
    assertEquals("valueEnd", params.get("column2"));
    assertEquals("value", params.get("column3"));

  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInColumnNames()
      throws QueryGrammarException {
    final SelectQuery query = select(func("myFunc", "param", 2)) //
        .from("table") //
        .where("column").eq(42);

    assertEquals(
        "SELECT myFunc(:myFunc1,:myFunc2) FROM table WHERE (column = :column3)",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("param", params.get("myFunc1"));
    assertEquals(2, params.get("myFunc2"));
    assertEquals(42, params.get("column3"));
  }

  // @SuppressWarnings("unchecked")
  // @Test
  // public void testShouldBuildSQLQueryWithMockFunctionInColumnNames() {
  // final Evaluable<String> funcMock = createMock(JavaSQLFunc.Evaluable.class);
  // expect(funcMock.eval("param", 1)).andReturn("1");
  // expect(funcMock.eval("param2", 2)).andReturn("2");
  // replay(funcMock);
  // Query.addFuncEvaluator("myFunc", funcMock);
  // final SelectQuery query = select(func("myFunc", "param", 1),
  // func("myFunc", "param2", 2)) //
  // .from("table") //
  // .where("column").eq(42);
  //
  // assertEquals("SELECT 1,2 FROM table WHERE (column = :column1)", query
  // .buildSQLQuery());
  // verify(funcMock);
  // final Map<String, Object> params = query.getParams();
  // assertEquals(1, params.size());
  // assertEquals(42, params.get("column1"));
  // Query.clearFuncEvaluatorMap();
  // }

  @Test
  public void testShouldBuildSQLQueryWithFunctionWithAliasInColumnNames()
      throws QueryGrammarException {
    final SelectQuery query = select(func("myFunc", "param", 2).as("myAlias")) //
        .from("table") //
        .where("column").eq(42);

    assertEquals(
        "SELECT myFunc(:myFunc1,:myFunc2) AS myAlias FROM table WHERE (column = :column3)",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("param", params.get("myFunc1"));
    assertEquals(2, params.get("myFunc2"));
    assertEquals(42, params.get("column3"));
  }

  @Test
  public void testShouldBuildSQLQueryWithColNameAndFunctionInColumnNames()
      throws QueryGrammarException {
    final SelectQuery query = select("col1", //
        func("myFunc", col("colparam"), 2), //
        "col3", //
        func("mySecondFunc", "param2", 22)) //
        .from("table") //
        .where("column").eq(42);

    final String sqlQuery = query.buildSQLQuery();
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
      throws QueryGrammarException {
    final SelectQuery query = select("*") //
        .from("table") //
        .where(func("upper", col("column"))).eq("AA");

    assertEquals("SELECT * FROM table WHERE (upper(column) = :upper1)", query
        .buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(1, params.size());
    assertEquals("AA", params.get("upper1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInEqClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*") //
        .from("table") //
        .where("column").eq(func("myFunc", "AA"));

    assertEquals("SELECT * FROM table WHERE (column = myFunc(:myFunc1))", query
        .buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(1, params.size());
    assertEquals("AA", params.get("myFunc1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithFunctionInAndClause()
      throws QueryGrammarException {
    final SelectQuery query = select("*") //
        .from("table") //
        .where("col").eq(42) //
        .and(func("upper", col("column"))).eq("AA");

    assertEquals(
        "SELECT * FROM table WHERE ((col = :col1) AND (upper(column) = :upper2))",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals(42, params.get("col1"));
    assertEquals("AA", params.get("upper2"));
  }

  @Test
  public void testShouldBuildSQLQueryWith1ValueAndEqualSign()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table") //
        .where("col").eq("val") //
        .and("column").betweenOrOp(Operator.EQ, "valueStart", "valueEnd") //
        .and("column").eq("value"); //

    assertEquals(
        "SELECT * FROM table WHERE (((col = :col1) AND (column = :column2)) AND (column = :column3))",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(3, params.size());
    assertEquals("val", params.get("col1"));
    assertEquals("valueStart", params.get("column2"));
    assertEquals("value", params.get("column3"));
  }

  @Test
  public void testShouldBuildSQLQueryWith2ValueAndBetweenSign()
      throws QueryGrammarException {
    final SelectQuery query = select("*").from("table").where("col").eq("val")
        .and("column").betweenOrOp(Operator.BTW, "valueStart", "valueEnd").and(
            "column").eq("value");

    assertEquals(
        "SELECT * FROM table WHERE (((col = :col1) AND (column BETWEEN :column2 and :column3)) AND (column = :column4))",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(4, params.size());
    assertEquals("val", params.get("col1"));
    assertEquals("valueStart", params.get("column2"));
    assertEquals("valueEnd", params.get("column3"));
    assertEquals("value", params.get("column4"));
  }

  @Test
  public void testShouldBuildSQLQueryWithBeanAnd() throws QueryGrammarException {
    final SelectQuery query = select("*").from("table") //
        .where("col").eq("val") //
        .and("column").betweenOrOp(Operator.BTW, "valueStart", "valueEnd") //
        .and("column").eq("value");

    assertEquals(
        "SELECT * FROM table WHERE (((col = :col1) AND (column BETWEEN :column2 and :column3)) AND (column = :column4))",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(4, params.size());
    assertEquals("val", params.get("col1"));
    assertEquals("valueStart", params.get("column2"));
    assertEquals("valueEnd", params.get("column3"));
    assertEquals("value", params.get("column4"));
  }

  @Test
  public void testShouldBuildSQLQueryWithUnionSelect()
      throws QueryGrammarException {
    final SelectQuery query = select("*")//
        .from("table") //
        .where("col").eq("val") //
        .union(//
            select("col1", "NULL col2") //
                .from("otherTable") //
                .where("col1").eq("val1") //
        );

    assertEquals(
        "SELECT * FROM table WHERE (col = :col1) UNION SELECT col1,NULL col2 FROM otherTable WHERE (col1 = :col12) ",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals("val1", params.get("col12"));
    assertEquals("val", params.get("col1"));
  }
}
