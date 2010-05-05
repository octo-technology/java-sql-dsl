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

package com.octo.java.sql.test;

import static com.octo.java.sql.query.Query.c;
import static com.octo.java.sql.query.Query.select;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.octo.java.sql.query.BasicQueryOptimizer;
import com.octo.java.sql.query.QueryException;
import com.octo.java.sql.query.SelectQuery;

public class BasicOptimizedSelectQueryTest {
  @Before
  public void setUp() {
    SelectQuery.addVisitor(new BasicQueryOptimizer());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereInClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).in((Object[]) null);

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(null);

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithOneWhereEqClauseAndANullableNullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eqNullable(null);

    assertEquals("SELECT * FROM table WHERE (column IS NULL)", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreTwoWhereClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(null) //
        .and(c("lastColumn")).eq("lastColumnValue");

    assertEquals("SELECT * FROM table WHERE ((lastColumn = :lastColumn1))",
        query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("lastColumnValue", query.getParams().get("lastColumn1"));
  }

  @Test
  public void testShouldBuildSQLQueryWithMoreThreeWhereClauseAndANullValue()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).eq(null) //
        .and(c("otherColumn")).eq(null) //
        .and(c("lastColumn")).eq("lastColumnValue");

    assertEquals("SELECT * FROM table WHERE ((lastColumn = :lastColumn1))",
        query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("lastColumnValue", query.getParams().get("lastColumn1"));
  }

  @Test
  public void testShouldBuildSQLQueryStartWithNullExp() throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).startWith(null);

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryStartWithEmptyString()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).startWith("");

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithContainsWithNull()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).contains(null);

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithContainsWithEmptyString()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).contains("");

    assertEquals("SELECT * FROM table", query.toSql());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldBuildSQLQueryWithoutBetweenSignWhenValuesAreNull()
      throws QueryException {
    final SelectQuery query = select("*").from("table") //
        .where(c("column")).between(null, null) //
        .and(c("column")).eq("value");

    assertEquals("SELECT * FROM table WHERE ((column = :column1))", query
        .toSql());
    final Map<String, Object> params = query.getParams();
    assertEquals(1, params.size());
    assertEquals("value", params.get("column1"));
  }
}
