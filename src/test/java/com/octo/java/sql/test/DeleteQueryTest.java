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
import static com.octo.java.sql.query.Query.deleteFrom;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.octo.java.sql.query.DeleteQuery;
import com.octo.java.sql.query.QueryException;

public class DeleteQueryTest {
  @Test
  public void testShouldCallStoredProcedureWithoutArgument()
      throws QueryException {
    final DeleteQuery query = deleteFrom("table") //
        .where(c("column")).eq("value");

    assertEquals("DELETE FROM table WHERE (column = :column1)", query.toSql());
    assertEquals(1, query.getParams().size());
    assertEquals("value", query.getParams().get("column1"));
  }
}
