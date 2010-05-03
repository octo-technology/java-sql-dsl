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

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.util.Collection;

import com.octo.java.sql.query.QueryGrammarException;
import com.octo.java.sql.query.visitor.Visitable;

public abstract class Exp implements Visitable {
  public AndExp and(final Column andColumn, final Operator andOperator,
      final Object andValue) {
    return new AndExp(this, new OpExp(andColumn, andOperator, andValue));
  }

  public AndExp and(final Column andColumn) {
    return new AndExp(this, new OpExp(andColumn));
  }

  public AndExp and(final SQLFunc func) {
    return new AndExp(this, new OpExp(func));
  }

  public AndExp and(final Exp exp) {
    return new AndExp(this, exp);
  }

  public OrExp or(final Column orColumn, final Operator orOperator,
      final Object orValue, final boolean orValueIsColumnName) {
    return new OrExp(this, new OpExp(orColumn, orOperator, orValue));
  }

  public OrExp or(final Column orColumn) {
    return new OrExp(this, new OpExp(orColumn));
  }

  public OrExp or(final Exp exp) {
    return new OrExp(this, exp);
  }

  public Exp eq(final Object value) throws QueryGrammarException {
    return applyOperation(Operator.EQ, value);
  }

  public Exp neq(final Object value) throws QueryGrammarException {
    return applyOperation(Operator.NEQ, value);
  }

  public Exp geq(final Long value) throws QueryGrammarException {
    return applyOperation(Operator.GEQ, value);
  }

  public Exp leq(final Long value) throws QueryGrammarException {
    return applyOperation(Operator.LEQ, value);
  }

  public Exp like(final String value) throws QueryGrammarException {
    return applyOperation(Operator.LIKE, value);
  }

  public Exp between(final Object valueStart, final Object valueEnd)
      throws QueryGrammarException {
    return applyBetweenOperation(valueStart, valueEnd);
  }

  public Exp startWith(String value) throws QueryGrammarException {
    value = isEmpty(value) ? null : value.concat("%");
    return applyOperation(Operator.LIKE, value);
  }

  public Exp contains(String value) throws QueryGrammarException {
    value = isEmpty(value) ? null : "%".concat(value.concat("%"));
    return applyOperation(Operator.LIKE, value);
  }

  public Exp in(final Object... values) throws QueryGrammarException {
    return applyInOperation(values);
  }

  public Exp in(final Collection<Object> values) throws QueryGrammarException {
    if (values == null) {
      return this;
    } else {
      return applyInOperation(values.toArray(new Object[values.size()]));
    }
  }

  public Exp notIn(final Object... values) throws QueryGrammarException {
    return applyNotInOperation(values);
  }

  public Exp notIn(final Collection<Object> values)
      throws QueryGrammarException {
    if (values == null) {
      return this;
    } else {
      return applyNotInOperation(values.toArray(new Object[values.size()]));
    }
  }

  public Exp isNull() throws QueryGrammarException {
    return applyOperation(Operator.IS, Constant.NULL);
  }

  public Exp isNotNull() throws QueryGrammarException {
    return applyOperation(Operator.IS_NOT, Constant.NULL);
  }

  /**
   * If operator is "between" do a between operation, else apply operation with
   * op.
   * 
   * @param op
   * @param valueStart
   * @param valueEnd
   * @return
   * @throws QueryGrammarException
   */
  public Exp betweenOrOp(final Operator op, final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    if (Operator.BTW.equals(op)) {
      return applyBetweenOperation(valueStart, valueEnd);
    } else {
      return applyOperation(op, valueStart);
    }
  }

  public abstract boolean isValid();

  public abstract Exp applyOperation(final Operator operator, final Object value)
      throws QueryGrammarException;

  public abstract Exp applyInOperation(final Object... values)
      throws QueryGrammarException;

  public abstract Exp applyNotInOperation(final Object... values)
      throws QueryGrammarException;

  public abstract Exp applyBetweenOperation(final Object valueStart,
      final Object valueEnd) throws QueryGrammarException;
}
