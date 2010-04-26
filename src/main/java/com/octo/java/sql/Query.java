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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.octo.java.sql.QueryPart.Operator;

public abstract class Query<T extends Query<T>> {
  /**
   * Logger for this class
   */
  private static final Logger logger = Logger.getLogger(Query.class);

  /**
   * Set it to false when using HSQLDB
   */
  public static boolean oracleDialect = true;
  private static Map<String, JavaSQLFunc.Evaluable<String>> funcEvaluatorMap = new HashMap<String, JavaSQLFunc.Evaluable<String>>();

  protected Exp whereClause;

  /**
   * @param columns
   *          to put both SQLFunc & Colsname
   */
  public static SelectQuery select(final Object... columnsAndFunc) {
    return new SelectQuery(columnsAndFunc);
  }

  /**
   * Create an UPDATE Query
   * 
   * @param table
   * @return
   */
  public static UpdateQuery update(final String table) {
    return new UpdateQuery(table);
  }

  /**
   * Create an INSERT Query
   * 
   * @param table
   * @return
   */
  public static InsertQuery insertInto(final String table) {
    return new InsertQuery(table);
  }

  /**
   * Create a CALL Query
   * 
   * @param procedure
   * @return
   */
  public static CallQuery call(final String procedure) {
    return new CallQuery(procedure);
  }

  /**
   * Create a CALL Query with parameters
   * 
   * @param procedure
   * @param params
   * @return
   */
  public static CallQuery call(final String procedure, final Object... params) {
    return new CallQuery(procedure, params);
  }

  /**
   * Create an SQLFunc
   * 
   * @param funcName
   * @param params
   * @return
   */
  public static SQLFunc func(final String funcName, final Object... params) {
    if (funcEvaluatorMap.containsKey(funcName)) {
      final JavaSQLFunc.Evaluable<String> evaluator = funcEvaluatorMap
          .get(funcName);
      return new JavaSQLFunc(funcName, params, evaluator);
    } else {
      return new SQLFunc(funcName, params);
    }
  }

  /**
   * Create a Column
   * 
   * @param name
   * @return
   */
  public static Column col(final String name) {
    return new Column(name);
  }

  /**
   * Add WHERE clause to SQL query being built
   * 
   * @param result
   * @throws QueryGrammarException
   */
  protected void buildWhereClause(final StringBuilder result)
      throws QueryGrammarException {
    if ((whereClause != null) && (whereClause.isValid())) {
      result.append(" WHERE ");
      whereClause.buildSQLQuery(result);
    }
  }

  /**
   * Add WHERE clause parameters to given result map
   * 
   * @param result
   */
  protected void getWhereParams(final Map<String, Object> result) {
    if ((whereClause != null) && (whereClause.isValid())) {
      whereClause.getParams(result);
    }
  }

  public String buildSQLQuery() throws QueryGrammarException {
    final StringBuilder result = new StringBuilder();
    QueryPart.resetVariableIndex();

    buildSQLQuery(result);

    final String sqlQuery = result.toString();
    if (logger.isDebugEnabled()) {
      logger.debug("buildSQLQuery() - String sqlQuery=" + sqlQuery);
    }
    return sqlQuery;
  }

  public abstract StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException;

  public abstract Map<String, Object> getParams();

  public abstract Map<String, Object> getParams(final Map<String, Object> result);

  @SuppressWarnings("unchecked")
  public T where(final String columnName) {
    whereClause = new ComparisonExp(columnName);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T where(final Exp newWhereClause) {
    this.whereClause = newWhereClause;
    return (T) this;
  }

  public T where(final String columnName, final String operator,
      final Object value) {
    return where(columnName, operator, value, false);
  }

  @SuppressWarnings("unchecked")
  public T where(final String columnName, final String operator,
      final Object value, final boolean valueIsColumnName) {

    if (value != null) {
      whereClause = new ComparisonExp(columnName, operator, value,
          valueIsColumnName, false);
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T where(final SQLFunc func) {
    whereClause = new ComparisonExp(func);
    return (T) this;
  }

  public T eq(final Object value) throws QueryGrammarException {
    return eq(value, false);
  }

  public T eq(final Query<?> nestedQuery) throws QueryGrammarException {
    return eq(nestedQuery, true);
  }

  @SuppressWarnings("unchecked")
  public T eq(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized("eq");
    whereClause.eq(value, valueIsColumnName);
    return (T) this;
  }

  public T neq(final Object value) throws QueryGrammarException {
    return neq(value, false);
  }

  public T neq(final Query<?> nestedQuery) throws QueryGrammarException {
    return neq(nestedQuery, true);
  }

  @SuppressWarnings("unchecked")
  public T neq(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized("neq");
    whereClause.neq(value, valueIsColumnName);
    return (T) this;
  }

  public T diff(final Object value) throws QueryGrammarException {
    return diff(value, false);
  }

  public T diff(final Query<?> nestedQuery) throws QueryGrammarException {
    return diff(nestedQuery, true);
  }

  @SuppressWarnings("unchecked")
  public T diff(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized("diff");
    whereClause.diff(value, valueIsColumnName);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T geq(final Long value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized("geq");
    whereClause.geq(value, valueIsColumnName);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T leq(final Long value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized("leq");
    whereClause.leq(value, valueIsColumnName);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T like(final String value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("like");
    whereClause = whereClause.like(value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T in(final Object... values) throws QueryGrammarException {
    assertWhereClauseIsInitialized("in");
    whereClause = whereClause.in(values);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T notIn(final Object... values) throws QueryGrammarException {
    assertWhereClauseIsInitialized("not in");
    whereClause = whereClause.notIn(values);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T and(final String columnName) throws QueryGrammarException {
    assertWhereClauseIsInitialized("and");
    whereClause = whereClause.and(columnName);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T and(final SQLFunc func) throws QueryGrammarException {
    assertWhereClauseIsInitialized("and");
    whereClause = whereClause.and(func);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T and(final Exp exp) throws QueryGrammarException {
    assertWhereClauseIsInitialized("and");
    whereClause = whereClause.and(exp);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T isNull() throws QueryGrammarException {
    assertWhereClauseIsInitialized("isNull");
    whereClause.isNull();
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T isNotNull() throws QueryGrammarException {
    assertWhereClauseIsInitialized("isNotNull");
    whereClause.isNotNull();
    return (T) this;
  }

  /**
   * @param op
   * @param valueStart
   * @param valueEnd
   * @return If operator is "between" do a between operation, else apply
   *         operation with op on valueStart.
   * @throws QueryGrammarException
   */
  @SuppressWarnings("unchecked")
  public T betweenOrOp(final Operator op, final Object valueStart,
      final Object valueEnd) throws QueryGrammarException {
    assertWhereClauseIsInitialized(op == null ? null : op.value);
    whereClause = whereClause.betweenOrOp(op, valueStart, valueEnd);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T between(final Object valueStart, final Object valueEnd)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized("between");
    whereClause = whereClause.between(valueStart, valueEnd);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T op(final Operator op, final Object value)
      throws QueryGrammarException {
    assertWhereClauseIsInitialized(op == null ? null : op.value);
    whereClause = whereClause.applyOperation(op, value, false);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T startWith(final String value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("startWith");
    whereClause = whereClause.startWith(value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T contains(final String value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("contains");
    whereClause = whereClause.contains(value);
    return (T) this;
  }

  public static void addFuncEvaluator(final String funcName,
      final JavaSQLFunc.Evaluable<String> evaluator) {
    funcEvaluatorMap.put(funcName, evaluator);
  }

  public static void clearFuncEvaluatorMap() {
    funcEvaluatorMap.clear();
  }

  private void assertWhereClauseIsInitialized(final String operation)
      throws QueryGrammarException {
    if (whereClause == null)
      throw new QueryGrammarException("Cannot apply '" + operation
          + "' operator if no where clause exist");
  }

}
