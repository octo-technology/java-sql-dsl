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

package com.octo.java.sql.exp;

import com.octo.java.sql.query.QueryException;
import com.octo.java.sql.query.QueryGrammarException;
import com.octo.java.sql.query.visitor.QueryVisitor;

public class InExp extends Exp {
  private final Column column;
  private final Object[] values;
  private final boolean negative;

  InExp(final Column column, final boolean negative, final Object... values) {
    super();
    this.column = column;
    this.values = values;
    this.negative = negative;
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
  public Exp applyOperation(final Operator operator, final Object value)
      throws QueryGrammarException {
    throw new QueryGrammarException("Cannot apply " + operator
        + " operation on an IN expression.");
  }

  @Override
  public Exp applyBetweenOperation(final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    throw new QueryGrammarException("Cannot apply IN on a BETWEEN expression.");
  }

  public void accept(final QueryVisitor visitor) throws QueryException {
    visitor.visit(this);
  }

  public Column getColumn() {
    return column;
  }

  public Object[] getValues() {
    return values;
  }

  public boolean isNegative() {
    return negative;
  }
}
