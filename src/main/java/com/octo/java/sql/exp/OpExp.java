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

import org.apache.commons.lang.StringUtils;

import com.octo.java.sql.query.QueryGrammarException;
import com.octo.java.sql.query.visitor.QueryVisitor;

public class OpExp extends Exp {
  private final Object lhsValue;
  private final Object rhsValue;
  private final Operator operator;

  public OpExp(final Object lhsValue) {
    super();
    this.lhsValue = lhsValue;
    this.rhsValue = null;
    this.operator = null;
  }

  public OpExp(final Object lhsValue, final Operator operator,
      final Object rhsValue) {
    super();
    this.lhsValue = lhsValue;
    this.rhsValue = rhsValue;
    this.operator = operator;
  }

  @Override
  public boolean isValid() {
    // Under Oracle, an empty string is considered to be null
    // So it's not possible to be equal to an empty string since it's not
    // possible to have one in db
    // So to be coherent, we skip the parameter also when the string is empty
    if (rhsValue instanceof String) {
      return StringUtils.isNotEmpty((String) rhsValue);
    }
    return rhsValue != null;
  }

  @Override
  public Exp applyOperation(final Operator newOperator, final Object newRhsValue)
      throws QueryGrammarException {
    if (operator != null) {
      throw new QueryGrammarException("Cannot apply " + newOperator.getValue()
          + " operation on an " + operator.getValue() + " expression.");
    }
    return new OpExp(lhsValue, newOperator, newRhsValue);
  }

  @Override
  public Exp applyInOperation(final Object... values)
      throws QueryGrammarException {
    if (!(lhsValue instanceof Column))
      throw new QueryGrammarException(
          "Can only apply 'in' operation on a Column");
    return new InExp((Column) lhsValue, false, values);
  }

  @Override
  public Exp applyNotInOperation(final Object... values)
      throws QueryGrammarException {
    if (!(lhsValue instanceof Column))
      throw new QueryGrammarException(
          "Can only apply 'in' operation on a Column");
    return new InExp((Column) lhsValue, true, values);
  }

  @Override
  public Exp applyBetweenOperation(final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    if (!(lhsValue instanceof Column))
      throw new QueryGrammarException(
          "Can only apply 'between' operation on a Column");
    return new BetweenExp((Column) lhsValue, valueStart, valueEnd);
  }

  public void accept(final QueryVisitor visitor) {
    visitor.visit(this);
  }

  public Object getLhsValue() {
    return lhsValue;
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getRhsValue() {
    return rhsValue;
  }
}
