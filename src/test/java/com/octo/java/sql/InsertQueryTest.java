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

import static com.octo.java.sql.Query.insertInto;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class InsertQueryTest {
  @Test
  public void testShouldBuildInsertSQLQuery() throws QueryGrammarException {
    final InsertQuery query = insertInto("table").set("column1", 42).set(
        "column2", "value2");

    assertEquals(
        "INSERT INTO table (column1, column2) VALUES (:column1, :column2)",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals(42, params.get("column1"));
    assertEquals("value2", params.get("column2"));
  }

  @Test
  public void testShouldBuildInsertSQLQueryInvertedColumns()
      throws QueryGrammarException {
    final InsertQuery query = insertInto("table").set("column2", "value2").set(
        "column1", 42);

    assertEquals(
        "INSERT INTO table (column2, column1) VALUES (:column2, :column1)",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals(42, params.get("column1"));
    assertEquals("value2", params.get("column2"));
  }

  @Test
  public void testShouldBuildInsertSQLQueryWithDefaultValue()
      throws QueryGrammarException {
    final InsertQuery query = insertInto("table").set("column1", null, "").set(
        "column2", null);

    assertEquals(
        "INSERT INTO table (column1, column2) VALUES (:column1, :column2)",
        query.buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals("", params.get("column1"));
    assertEquals(null, params.get("column2"));
  }
}
