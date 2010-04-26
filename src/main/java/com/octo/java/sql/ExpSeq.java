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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class ExpSeq extends Exp {
  protected final List<Exp> clauses;

  public ExpSeq(final Exp... clauses) {
    super();
    this.clauses = Arrays.asList(clauses);
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    result.append(OPEN_BRACKET);
    final String operator = getOperator();
    boolean firstClause = true;
    for (final Exp clause : clauses) {
      if (clause.isValid()) {
        if (firstClause) {
          firstClause = false;
        } else {
          result.append(" ").append(operator).append(" ");
        }
        clause.buildSQLQuery(result);
      }
    }
    result.append(CLOSE_BRACKET);
    return result;
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    for (final Exp clause : clauses) {
      if (clause.isValid()) {
        clause.getParams(result);
      }
    }
    return result;
  }

  @Override
  public boolean isValid() {
    boolean result = false;
    for (final Exp clause : clauses) {
      result |= clause.isValid();
    }
    return result;
  }

  @Override
  public Exp applyOperation(final String operator, final Object value,
      final boolean valueIsColumnName) throws QueryGrammarException {
    final Exp lastClause = clauses.get(clauses.size() - 1);
    lastClause.applyOperation(operator, value, valueIsColumnName);
    return this;
  }

  @Override
  public Exp applyBetweenOperation(final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    final int lastIndex = clauses.size() - 1;
    final Exp lastClause = clauses.get(lastIndex);
    clauses.set(lastIndex, lastClause.applyBetweenOperation(valueStart,
        valueEnd));
    return this;
  }

  @Override
  public Exp applyInOperation(final Object... values)
      throws QueryGrammarException {
    final int lastIndex = clauses.size() - 1;
    final Exp lastClause = clauses.get(lastIndex);
    clauses.set(lastIndex, lastClause.applyInOperation(values));
    return this;
  }

  @Override
  public Exp applyNotInOperation(final Object... values)
      throws QueryGrammarException {
    final int lastIndex = clauses.size() - 1;
    final Exp lastClause = clauses.get(lastIndex);
    clauses.set(lastIndex, lastClause.applyNotInOperation(values));
    return this;
  }

  public abstract String getOperator();
}
