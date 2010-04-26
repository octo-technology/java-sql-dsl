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

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.util.Collection;

public abstract class Exp extends QueryPart {
  private static final String ORDER_BY = " orderBy ";
  public static final String NOT_NULL = "NOT NULL";
  public static final String NULL = "NULL";
  public static final String OPEN_BRACKET = "(";
  public static final String CLOSE_BRACKET = ")";

  public AndExp and(final String andColumnName, final String andOperator,
      final Object andValue, final boolean andValueIsColumnName) {
    return new AndExp(this, new ComparisonExp(andColumnName, andOperator,
        andValue, andValueIsColumnName, false));
  }

  public AndExp and(final String andColumnName) {
    return new AndExp(this, new ComparisonExp(andColumnName));
  }

  public AndExp and(final SQLFunc func) {
    return new AndExp(this, new ComparisonExp(func));
  }

  public AndExp and(final Exp exp) {
    return new AndExp(this, exp);
  }

  public OrExp or(final String orColumnName, final String orOperator,
      final Object orValue, final boolean orValueIsColumnName) {
    return new OrExp(this, new ComparisonExp(orColumnName, orOperator, orValue,
        orValueIsColumnName, false));
  }

  public OrExp or(final String orColumnName) {
    return new OrExp(this, new ComparisonExp(orColumnName));
  }

  public OrExp or(final Exp exp) {
    return new OrExp(this, exp);
  }

  public Exp eq(final Object value) throws QueryGrammarException {
    return eq(value, false);
  }

  public Exp eq(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    return applyOperation(Operator.EQ, value, valueIsColumnName);
  }

  public Exp neq(final Object value) throws QueryGrammarException {
    return neq(value, false);
  }

  public Exp neq(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    return applyOperation(Operator.NEQ, value, valueIsColumnName);
  }

  public Exp geq(final Long value) throws QueryGrammarException {
    return geq(value, false);
  }

  public Exp geq(final Long value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    return applyOperation(Operator.GEQ, value, valueIsColumnName);
  }

  public Exp leq(final Long value) throws QueryGrammarException {
    return leq(value, false);
  }

  public Exp leq(final Long value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    return applyOperation(Operator.LEQ, value, valueIsColumnName);
  }

  public Exp diff(final Object value) throws QueryGrammarException {
    return diff(value, false);
  }

  public Exp diff(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    return applyOperation(Operator.NEQ, value, valueIsColumnName);
  }

  public Exp like(final String value) throws QueryGrammarException {
    return like(value, false);
  }

  public Exp like(final String value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    return applyOperation(Operator.LIKE, value, valueIsColumnName);
  }

  public Exp between(final Object valueStart, final Object valueEnd)
      throws QueryGrammarException {
    return applyBetweenOperation(valueStart, valueEnd);
  }

  public Exp startWith(final String value) throws QueryGrammarException {
    return startWith(value, false);
  }

  public Exp startWith(String value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    value = isEmpty(value) ? null : value.concat("%");
    return applyOperation(Operator.LIKE, value, valueIsColumnName);
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
    return applyOperation(Operator.IS, NULL, true);
  }

  public Exp isNotNull() throws QueryGrammarException {
    return applyOperation(Operator.IS, NOT_NULL, true);
  }

  public Exp orderBy(final String value) throws QueryGrammarException {
    return applyOperation(ORDER_BY, value, false);
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
      return applyOperation(op, valueStart, false);
    }
  }

  public Exp applyOperation(final Operator op, final Object value,
      final boolean valueIsColumnName) throws QueryGrammarException {
    if (op == null) {
      return this;
    } else {
      return applyOperation(op.value, value, valueIsColumnName);
    }
  }

  public abstract boolean isValid();

  public abstract Exp applyOperation(String operator, Object value,
      boolean valueIsColumnName) throws QueryGrammarException;

  public abstract Exp applyInOperation(Object... values)
      throws QueryGrammarException;

  public abstract Exp applyNotInOperation(Object... values)
      throws QueryGrammarException;

  public abstract Exp applyBetweenOperation(Object valueStart, Object valueEnd)
      throws QueryGrammarException;

  public Exp contains(String value) throws QueryGrammarException {
    value = isEmpty(value) ? null : "%".concat(value.concat("%"));
    return applyOperation(Operator.LIKE, value, false);
  }
}
