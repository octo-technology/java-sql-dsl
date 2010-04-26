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

import static org.apache.commons.collections.CollectionUtils.collect;
import static org.apache.commons.lang.StringUtils.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InExp extends Exp {
  private final String columnName;
  private final Object[] values;
  private final Map<String, Object> params = new HashMap<String, Object>();
  private final boolean negate;

  InExp(final String columnName, final boolean negate, final Object... values) {
    super();
    this.columnName = columnName;
    this.values = values;
    this.negate = negate;
  }

  @Override
  public Exp applyInOperation(final Object... newValues)
      throws QueryGrammarException {
    throw new QueryGrammarException(
        "Cannot apply IN operation on an IN expression.");
  }

  @Override
  public Exp applyNotInOperation(final Object... values)
      throws QueryGrammarException {
    throw new QueryGrammarException(
        "Cannot apply NOT IN operation on an IN expression.");
  }

  @Override
  public Exp applyOperation(final String operator, final Object value,
      final boolean valueIsColumnName) throws QueryGrammarException {
    throw new QueryGrammarException("Cannot apply " + operator
        + " operation on an IN expression.");
  }

  @Override
  public Exp applyBetweenOperation(final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    throw new QueryGrammarException("Cannot apply IN on a BETWEEN expression.");
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    final List<String> variables = new ArrayList<String>();
    result.append(OPEN_BRACKET);
    result.append(columnName).append(" ");
    if (negate) {
      result.append(Operator.NOT.value).append(" ");
    }
    result.append(Operator.IN.value).append(" (");
    if ((values.length == 1) && (values[0] instanceof Query<?>)) {
      result.append(((Query<?>) values[0]).buildSQLQuery(new StringBuilder()));
    } else {
      for (final Object value : values) {
        final String variable = getVariableName(columnName);
        variables.add(variable);
        params.put(variable, value);
      }
      result.append(join(collect(variables, variableTransformer), ','));
    }
    result.append(CLOSE_BRACKET);
    return result.append(CLOSE_BRACKET);
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    if ((values.length == 1) && (values[0] instanceof Query<?>)) {
      result.putAll(((Query<?>) values[0]).getParams());
    } else {
      result.putAll(params);
    }
    return result;
  }

  @Override
  public boolean isValid() {
    return (values != null) && (values.length > 0)
        && (!((values.length == 1) && (values[0] == null)));
  }
}
