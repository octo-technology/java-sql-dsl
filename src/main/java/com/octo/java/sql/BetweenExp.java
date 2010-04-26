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

public class BetweenExp extends Exp {
  private static final String BETWEEN = "BETWEEN";
  private final String columnName;
  private final Object valueStart;
  private String valueStartName = null;
  private final Object valueEnd;
  private String valueEndName = null;

  public BetweenExp(final String columnName, final Object valueStart,
      final Object valueEnd) {
    super();
    this.columnName = columnName;
    this.valueStart = valueStart;
    this.valueEnd = valueEnd;
  }

  @Override
  public Exp applyInOperation(final Object... newValues)
      throws QueryGrammarException {
    throw new QueryGrammarException(
        "Cannot apply BETWENN operation on an IN expression.");
  }

  @Override
  public Exp applyNotInOperation(final Object... values)
      throws QueryGrammarException {
    throw new QueryGrammarException(
        "Cannot apply BETWENN operation on an NOT IN expression.");
  }

  @Override
  public Exp applyOperation(final String operator, final Object value,
      final boolean valueIsColumnName) throws QueryGrammarException {
    throw new QueryGrammarException("Cannot apply " + operator
        + " operation on an BETWEEN expression.");
  }

  @Override
  public Exp applyBetweenOperation(final Object start, final Object end)
      throws QueryGrammarException {
    throw new QueryGrammarException(
        "Cannot apply BETWEEN on a BETWEEN expression.");
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result) {

    valueStartName = getVariableName(columnName);
    valueEndName = getVariableName(columnName);

    result.append(OPEN_BRACKET);
    result.append(columnName).append(" ").append(BETWEEN).append(" ");
    result.append(":").append(valueStartName);
    result.append(" and ");
    result.append(":").append(valueEndName);
    return result.append(CLOSE_BRACKET);
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    result.put(valueStartName, valueStart);
    result.put(valueEndName, valueEnd);
    return result;
  }

  @Override
  public boolean isValid() {
    return ((valueStart != null) && (valueEnd != null));
  }

}
