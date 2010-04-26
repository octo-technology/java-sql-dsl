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

import static com.octo.java.sql.Query.call;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class CallQueryTest {
  @Test
  public void testShouldCallStoredProcedureWithoutArgument()
      throws QueryGrammarException {
    final CallQuery query = call("myStoredProcedure");

    assertEquals("BEGIN myStoredProcedure();END;", query.buildSQLQuery());
    assertEquals(0, query.getParams().size());
  }

  @Test
  public void testShouldCallStoredProcedureWithArguments()
      throws QueryGrammarException {
    final CallQuery query = call("myStoredProcedure", "arg1", 42);

    assertEquals("BEGIN myStoredProcedure(:param0,:param1);END;", query
        .buildSQLQuery());
    final Map<String, Object> params = query.getParams();
    assertEquals(2, params.size());
    assertEquals("arg1", params.get("param0"));
    assertEquals(42, params.get("param1"));
  }
}
