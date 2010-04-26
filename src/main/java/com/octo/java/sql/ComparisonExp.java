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

import org.apache.commons.lang.StringUtils;

public class ComparisonExp extends Exp {
  private final String columnName;
  private String operator = null;
  private Object value;
  private boolean valueIsColumnName;
  private String variableName = null;
  private boolean isSetExpression;
  private final SQLFunc func;

  public ComparisonExp(final String columnName) {
    super();
    this.columnName = columnName;
    this.func = null;
  }

  public ComparisonExp(final SQLFunc func) {
    super();
    this.columnName = null;
    this.func = func;
  }

  public ComparisonExp(final String columnName, final String operator,
      final Object value, final boolean valueIsColumnName,
      final boolean isSetExpression) {
    super();
    this.columnName = columnName;
    this.operator = operator;
    this.value = value;
    this.valueIsColumnName = valueIsColumnName;
    this.isSetExpression = isSetExpression;
    this.func = null;
  }

  public ComparisonExp(final String columnName, final Operator operator,
      final Object value, final boolean valueIsColumnName,
      final boolean isSetExpression) {
    this(columnName, operator == null ? null : operator.value, value,
        valueIsColumnName, isSetExpression);
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    if (!isSetExpression) {
      result.append(OPEN_BRACKET);
    }
    if (columnName != null) {
      result.append(columnName);
    } else if (func != null) {
      func.buildSQLQuery(result);
    }
    result.append(" ").append(operator).append(" ");
    if (value instanceof Query<?>) {
      result.append(OPEN_BRACKET) //
          .append(((Query<?>) value).buildSQLQuery(new StringBuilder())) //
          .append(CLOSE_BRACKET);
    } else if (value instanceof SQLFunc) {
      ((SQLFunc) value).buildSQLQuery(result);
    } else {
      if (valueIsColumnName) {
        result.append(value);
      } else {
        if (func != null) {
          variableName = getVariableName(func.getName());
        } else {
          variableName = getVariableName(columnName);
        }
        // reuse columnName as variableName
        result.append(":").append(variableName);
      }
    }
    if (!isSetExpression) {
      result.append(CLOSE_BRACKET);
    }
    return result;
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    if (value instanceof Query<?>) {
      result.putAll(((Query<?>) value).getParams());
    } else if (value instanceof SQLFunc) {
      ((SQLFunc) value).getParams(result);
    } else if (!valueIsColumnName) {
      result.put(variableName, value);
    }
    return result;
  }

  @Override
  public boolean isValid() {
    // Under Oracle, an empty string is considered to be null
    // So it's not possible to be equal to an empty string since it's not
    // possible to have one in db
    // So to be coherent, we skip the parameter also when the string is empty
    if (value instanceof String) {
      return StringUtils.isNotEmpty((String) value);
    }
    return value != null;
  }

  @Override
  public Exp applyOperation(final String newOperator, final Object newValue,
      final boolean newValueIsColumnName) throws QueryGrammarException {
    if (operator != null) {
      throw new QueryGrammarException("Cannot apply " + newOperator
          + " operation on an " + operator + " expression.");
    }
    this.operator = newOperator;
    this.value = newValue;
    this.valueIsColumnName = newValueIsColumnName;
    return this;
  }

  @Override
  public Exp applyInOperation(final Object... values) {
    return new InExp(columnName, false, values);
  }

  @Override
  public Exp applyNotInOperation(final Object... values)
      throws QueryGrammarException {
    return new InExp(columnName, true, values);
  }

  @Override
  public Exp applyBetweenOperation(final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    return new BetweenExp(columnName, valueStart, valueEnd);
  }
}
