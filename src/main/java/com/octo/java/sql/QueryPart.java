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

import java.util.Map;

import org.apache.commons.collections.Transformer;

public abstract class QueryPart {
  public static enum Operator {
    IS("IS"), BTW("BETWEEN"), LIKE("LIKE"), EQ("="), NEQ("!="), GEQ(">="), LEQ(
        "<="), NOT("NOT"), IN("IN");

    public final String value;

    Operator(final String value) {
      this.value = value;
    }
  }

  private static int variableIndex = 1;

  public static final Transformer variableTransformer = new Transformer() {
    public Object transform(final Object input) {
      return ":" + input;
    }
  };

  public static void resetVariableIndex() {
    variableIndex = 1;
  }

  protected String getVariableName(final String columnName) {
    return columnName + variableIndex++;
  }

  @Override
  public String toString() {
    try {
      return "[" + getClass() + "]: " + buildSQLQuery(new StringBuilder());
    } catch (final QueryGrammarException e) {
      e.printStackTrace();
      return "[" + getClass() + "]: Cannot build SQL query: ";
    }
  }

  public abstract StringBuilder buildSQLQuery(StringBuilder result)
      throws QueryGrammarException;

  public abstract Map<String, Object> getParams(Map<String, Object> result);
}
