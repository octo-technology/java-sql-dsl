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

import java.util.Arrays;
import java.util.List;

import com.octo.java.sql.query.QueryGrammarException;
import com.octo.java.sql.query.visitor.QueryVisitor;

public abstract class ExpSeq extends Exp {
  protected final List<Exp> clauses;

  ExpSeq(final Exp... clauses) {
    super();
    this.clauses = Arrays.asList(clauses);
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
  public Exp applyOperation(final Operator operator, final Object value)
      throws QueryGrammarException {
    final int lastIndex = clauses.size() - 1;
    final Exp lastClause = clauses.get(lastIndex);
    clauses.set(lastIndex, lastClause.applyOperation(operator, value));
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

  public void accept(final QueryVisitor visitor) {
    visitor.visit(this);
  }

  public abstract Operator getOperator();

  public List<Exp> getClauses() {
    return clauses;
  }
}
