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

package com.octo.java.sql.query.visitor;

import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang.ArrayUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.join;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;

import com.octo.java.sql.exp.BetweenExp;
import com.octo.java.sql.exp.Column;
import com.octo.java.sql.exp.Constant;
import com.octo.java.sql.exp.Exp;
import com.octo.java.sql.exp.ExpSeq;
import com.octo.java.sql.exp.InExp;
import com.octo.java.sql.exp.JoinClause;
import com.octo.java.sql.exp.Nullable;
import com.octo.java.sql.exp.OpExp;
import com.octo.java.sql.exp.Operator;
import com.octo.java.sql.exp.SQLFunc;
import com.octo.java.sql.exp.SetClause;
import com.octo.java.sql.exp.JavaSQLFunc.Evaluable;
import com.octo.java.sql.query.DeleteQuery;
import com.octo.java.sql.query.InsertQuery;
import com.octo.java.sql.query.QueryException;
import com.octo.java.sql.query.QueryGrammarException;
import com.octo.java.sql.query.SelectQuery;
import com.octo.java.sql.query.UpdateQuery;
import com.octo.java.sql.query.SelectQuery.Order;

public class DefaultQueryBuilder extends BaseVisitor {
  public static final String DEFAULT_BASE_VARIABLE_NAME = "param";
  private static final String OPEN_BRACKET = "(";
  private static final String BETWEEN = "BETWEEN";
  private static final String CLOSE_BRACKET = ")";
  private static final String ON = "ON";
  private static final String AS = "AS";
  private static final String SELECT = "SELECT";
  private static final String FROM = "FROM";
  private static final String WHERE = "WHERE";
  private static final String ORDER_BY = "ORDER BY";
  private static final String UNION = "UNION";
  private static final String INSERT = "INSERT INTO";
  private static final String VALUES = "VALUES";
  private static final String UPDATE = "UPDATE";
  private static final String SET = "SET";
  private static final String DELETE_FROM = "DELETE FROM";

  protected final StringBuilder result = new StringBuilder();
  private int variableIndex = 1;
  private final Map<String, Object> params = new HashMap<String, Object>();
  private boolean addBracketToNextSelectQuery = false;
  private final Map<String, Evaluable<?>> functions = new HashMap<String, Evaluable<?>>();

  public void addFunction(final String functionName,
      final Evaluable<String> function) {
    functions.put(functionName, function);
  }

  public StringBuilder getResult() {
    return result;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  /**
   * Add a variable to parameters map and return its name
   * 
   * @param columnName
   * @return
   */
  protected String addVariable(final Object value, final String baseColumnName) {
    final String columnName = baseColumnName + variableIndex++;
    params.put(columnName, value);
    return columnName;
  }

  private String getVariableName(final Object obj, final String defaultName) {
    if (obj instanceof Column)
      return ((Column) obj).getName();
    else if (obj instanceof SQLFunc)
      return ((SQLFunc) obj).getName();
    else
      return defaultName;
  }

  private void acceptOrVisitValue(final Object value, final String baseName)
      throws QueryException {
    if (value instanceof Visitable)
      ((Visitable) value).accept(this);
    else {
      final String variableName = addVariable(value, baseName);
      result.append(":").append(variableName);
    }
  }

  public void visitValue(final Object value) {
    final String variableName = addVariable(value, DEFAULT_BASE_VARIABLE_NAME);
    result.append(":").append(variableName);
  }

  protected void buildWhereClause(final Exp whereClause) throws QueryException {
    if (whereClause.isValid()) {
      result.append(" ").append(WHERE).append(" ");
      whereClause.accept(this);
    }
  }

  protected void buildLimitClause(final SelectQuery query) {
    result.append(" ").append("LIMIT").append(" ");
    result.append(":").append(addVariable(query.getLimit(), "limit"));
  }

  /**
   * Visit methods
   */
  public void visit(final Column column) {
    result.append(column.getName());
  }

  public void visit(final OpExp exp) throws QueryException {
    String baseVariableName = getVariableName(exp.getLhsValue(), null);
    if (baseVariableName == null)
      baseVariableName = getVariableName(exp.getRhsValue(), "var");

    result.append(OPEN_BRACKET);
    acceptOrVisitValue(exp.getLhsValue(), baseVariableName);
    if ((exp.getRhsValue() == null) //
        || ((exp.getRhsValue() instanceof Nullable) //
        && ((Nullable) exp.getRhsValue()).isNull())) {
      if (!Operator.EQ.equals(exp.getOperator()))
        throw new QueryGrammarException("Cannot use NULL value with operator "
            + exp.getOperator().getValue());
      result.append(" ").append(Operator.IS.getValue());
      result.append(" ").append(Constant.NULL);
    } else {
      result.append(" ").append(exp.getOperator().getValue()).append(" ");
      acceptOrVisitValue(exp.getRhsValue(), baseVariableName);
    }
    result.append(CLOSE_BRACKET);
  }

  public void visit(final BetweenExp betweenExp) throws QueryException {
    final Column column = betweenExp.getColumn();
    if ((betweenExp.getValueStart() == null)
        || (betweenExp.getValueEnd() == null))
      throw new QueryGrammarException(
          "Cannot apply BETWEEN with one NULL value");

    result.append(OPEN_BRACKET);
    visit(column);
    result.append(" ").append(BETWEEN).append(" ");
    acceptOrVisitValue(betweenExp.getValueStart(), column.getName());
    result.append(" ").append(Operator.AND).append(" ");
    acceptOrVisitValue(betweenExp.getValueEnd(), column.getName());
    result.append(CLOSE_BRACKET);
  }

  public void visit(final ExpSeq expSeq) throws QueryException {
    result.append(OPEN_BRACKET);
    final Operator operator = expSeq.getOperator();
    boolean firstClause = true;
    for (final Exp clause : expSeq.getClauses()) {
      if (clause.isValid()) {
        if (firstClause)
          firstClause = false;
        else
          result.append(" ").append(operator.getValue()).append(" ");
        clause.accept(this);
      }
    }
    result.append(CLOSE_BRACKET);
  }

  public void visit(final InExp inExp) throws QueryException {
    if (isEmpty(inExp.getValues()))
      throw new QueryGrammarException("IN values cannot be empty or null");

    result.append(OPEN_BRACKET);
    inExp.getColumn().accept(this);
    result.append(" ");
    if (inExp.isNegative())
      result.append(Operator.NOT.getValue()).append(" ");
    result.append(Operator.IN.getValue()).append(" ").append(OPEN_BRACKET);
    boolean firstValue = true;
    for (final Object value : inExp.getValues()) {
      if (firstValue)
        firstValue = false;
      else
        result.append(",");
      acceptOrVisitValue(value, inExp.getColumn().getName());
    }
    result.append(CLOSE_BRACKET);
    result.append(CLOSE_BRACKET);
  }

  public void visit(final JoinClause joinClause) throws QueryException {
    result.append(" ").append(joinClause.getType().value).append(" ");
    result.append(joinClause.getTable());
    result.append(" ").append(ON).append(" ");
    joinClause.getOnClause().accept(this);
  }

  public void visit(final SetClause setClause) throws QueryException {
    final Column column = setClause.getColumn();
    column.accept(this);
    result.append(" ").append(Operator.EQ.getValue()).append(" ");
    acceptOrVisitValue(setClause.getValue(), column.getName());
  }

  public void visit(final SQLFunc sqlFunc) throws QueryException {
    final String functionName = sqlFunc.getName();
    if (functions.containsKey(functionName)) {
      final Evaluable<?> functionPlaceHolder = functions.get(functionName);
      result.append(functionPlaceHolder.eval(sqlFunc.getParams()));
    } else {
      result.append(functionName).append(OPEN_BRACKET);
      boolean firstParam = true;
      for (final Object param : sqlFunc.getParams()) {
        if (firstParam)
          firstParam = false;
        else
          result.append(",");
        acceptOrVisitValue(param, functionName);
      }
      result.append(CLOSE_BRACKET);
      if (!isEmpty(sqlFunc.getAlias()))
        result.append(" ").append(AS).append(" ").append(sqlFunc.getAlias());
    }
  }

  public void visit(final Constant constant) {
    result.append(constant.getValue());
  }

  public void visit(final SelectQuery query) throws QueryException {
    final boolean innerQuery = addBracketToNextSelectQuery;
    if (innerQuery)
      result.append(OPEN_BRACKET);
    else
      addBracketToNextSelectQuery = true;
    result.append(SELECT).append(" ");
    boolean firstColumn = true;
    for (final Object column : query.getColumns()) {
      if (firstColumn)
        firstColumn = false;
      else
        result.append(",");
      acceptOrVisitValue(column);
    }

    result.append(" ").append(FROM).append(" ");
    result.append(join(query.getTables(), ','));

    for (final JoinClause clause : query.getJoinClauses())
      if (clause.isValid())
        clause.accept(this);

    if (!isEmpty(query.getAlias())) {
      result.append(" ");
      result.append(query.getAlias());
    }

    final Exp whereClause = query.getWhereClause();
    if ((whereClause != null) && (whereClause.isValid()))
      buildWhereClause(whereClause);

    boolean firstOrderBy = true;
    final Map<String, Order> orderBy = query.getOrderBy();
    for (final String orderByColumn : orderBy.keySet()) {
      if (firstOrderBy) {
        result.append(" ").append(ORDER_BY).append(" ");
        firstOrderBy = false;
      } else
        result.append(", ");
      result.append(orderByColumn);
      final Order columnOrder = orderBy.get(orderByColumn);
      if (columnOrder != null)
        result.append(" ").append(columnOrder.toString());
    }

    if (query.getLimit() != null)
      buildLimitClause(query);

    if (!isEmpty(query.getUnions())) {
      result.append(" ").append(UNION).append(" ");
      for (final SelectQuery union : query.getUnions()) {
        addBracketToNextSelectQuery = false;
        union.accept(this);
      }
    }
    if (innerQuery)
      result.append(CLOSE_BRACKET);
  }

  public void visit(final UpdateQuery updateQuery) throws QueryException {
    addBracketToNextSelectQuery = true;
    result.append(UPDATE).append(" ");
    result.append(updateQuery.getTable());
    result.append(" ").append(SET).append(" ");
    boolean firstClause = true;
    for (final SetClause clause : updateQuery.getSetClauses()) {
      if (firstClause) {
        firstClause = false;
      } else {
        result.append(", ");
      }
      clause.accept(this);
    }

    final Exp whereClause = updateQuery.getWhereClause();
    if ((whereClause != null) && (whereClause.isValid()))
      buildWhereClause(whereClause);
  }

  public void visit(final InsertQuery insertQuery) throws QueryException {
    addBracketToNextSelectQuery = true;
    result.append(INSERT).append(" ");
    result.append(insertQuery.getTable()).append(" ");
    result.append(OPEN_BRACKET);
    final ListOrderedMap columnValues = insertQuery.getColumnsValues();
    result.append(join(columnValues.keyList(), ", "));
    result.append(CLOSE_BRACKET).append(" ");
    result.append(VALUES).append(" ").append(OPEN_BRACKET);
    boolean firstClause = true;
    for (final Object column : columnValues.keyList()) {
      if (firstClause) {
        firstClause = false;
      } else {
        result.append(", ");
      }
      acceptOrVisitValue(columnValues.get(column), (String) column);
    }
    result.append(CLOSE_BRACKET);
  }

  public void visit(final DeleteQuery deleteQuery) throws QueryException {
    addBracketToNextSelectQuery = true;
    result.append(DELETE_FROM).append(" ");
    result.append(join(deleteQuery.getTables(), ','));

    final Exp whereClause = deleteQuery.getWhereClause();
    if ((whereClause != null) && (whereClause.isValid()))
      buildWhereClause(whereClause);
  }

  public void visit(final Nullable nullable) throws QueryException {
    acceptOrVisitValue(nullable.getValue());
  }
}
