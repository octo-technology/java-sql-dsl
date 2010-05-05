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

package com.octo.java.sql.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.octo.java.sql.exp.Column;
import com.octo.java.sql.exp.Exp;
import com.octo.java.sql.exp.JavaSQLFunc;
import com.octo.java.sql.exp.Nullable;
import com.octo.java.sql.exp.OpExp;
import com.octo.java.sql.exp.Operator;
import com.octo.java.sql.exp.SQLFunc;
import com.octo.java.sql.query.visitor.DefaultQueryBuilder;
import com.octo.java.sql.query.visitor.QueryVisitor;
import com.octo.java.sql.query.visitor.Visitable;

public abstract class Query<T extends Query<T>> implements Visitable {
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

  private static Class<? extends DefaultQueryBuilder> querybuilderClass = DefaultQueryBuilder.class;

  private static Set<QueryVisitor> visitors = new HashSet<QueryVisitor>();
  private DefaultQueryBuilder builder;

  public static void setDefaultQueryBuilder(
      final Class<? extends DefaultQueryBuilder> queryBuilderClass) {
    querybuilderClass = queryBuilderClass;
  }

  public static void resetDefaultQueryBuilder() {
    querybuilderClass = DefaultQueryBuilder.class;
  }

  public Exp getWhereClause() {
    return whereClause;
  }

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
   * Create an DELETE Query
   * 
   * @param table
   * @return
   */
  public static DeleteQuery deleteFrom(final String table) {
    return new DeleteQuery(table);
  }

  /**
   * Create an SQLFunc
   * 
   * @param funcName
   * @param params
   * @return
   */
  public static SQLFunc f(final String funcName, final Object... params) {
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
  public static Column c(final String name) {
    return new Column(name);
  }

  /**
   * Create a new OpExp
   * 
   * @param columnName
   * @return
   */
  public static OpExp e(final Column columnName) {
    return new OpExp(columnName);
  }

  public static OpExp e(final SQLFunc func) {
    return new OpExp(func);
  }

  public String toSql() throws QueryException {
    return toSql(getQueryBuilder());
  }

  public String toSql(final DefaultQueryBuilder queryBuilder)
      throws QueryException {
    runVisitors();
    builder = queryBuilder;
    accept(builder);
    final String sqlQuery = builder.getResult().toString();
    if (logger.isDebugEnabled())
      logger.debug("buildSQLQuery() - String sqlQuery=" + sqlQuery);
    return sqlQuery;
  }

  public DefaultQueryBuilder getQueryBuilder() throws QueryException {
    try {
      return querybuilderClass.newInstance();
    } catch (final InstantiationException e) {
      throw new QueryException("Cannot instanciate query builder "
          + querybuilderClass);
    } catch (final IllegalAccessException e) {
      throw new QueryException("Cannot instanciate query builder "
          + querybuilderClass);
    }
  }

  private void runVisitors() throws QueryException {
    for (final QueryVisitor visitor : visitors) {
      accept(visitor);
    }
  }

  public Map<String, Object> getParams() {
    return builder.getParams();
  }

  public static void addVisitor(final QueryVisitor visitor) {
    visitors.add(visitor);
  }

  public static void clearVisitors() {
    visitors.clear();
  }

  @SuppressWarnings("unchecked")
  public T where(final Column column) {
    whereClause = new OpExp(column);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T where(final Exp newWhereClause) {
    whereClause = newWhereClause;
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T where(final Column column, final Operator operator,
      final Object value) {
    if (value != null)
      whereClause = new OpExp(column, operator, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T where(final SQLFunc func) {
    whereClause = new OpExp(func);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T eq(final Object value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("eq");
    whereClause = whereClause.eq(value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T eqNullable(final Object value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("eq");
    whereClause = whereClause.eq(new Nullable(value));
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T neq(final Object value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("neq");
    whereClause = whereClause.neq(value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T neqNullable(final Object value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("neq");
    whereClause = whereClause.neq(new Nullable(value));
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T geq(final Long value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("geq");
    whereClause = whereClause.geq(value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T leq(final Long value) throws QueryGrammarException {
    assertWhereClauseIsInitialized("leq");
    whereClause = whereClause.leq(value);
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
  public T and(final Column column) throws QueryGrammarException {
    assertWhereClauseIsInitialized("and");
    whereClause = whereClause.and(column);
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
    whereClause = whereClause.isNull();
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T isNotNull() throws QueryGrammarException {
    assertWhereClauseIsInitialized("isNotNull");
    whereClause = whereClause.isNotNull();
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
    assertWhereClauseIsInitialized(op == null ? null : op.getValue());
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
    assertWhereClauseIsInitialized(op == null ? null : op.getValue());
    whereClause = whereClause.applyOperation(op, value);
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
