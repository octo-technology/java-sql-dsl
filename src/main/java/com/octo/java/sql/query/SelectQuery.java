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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.octo.java.sql.exp.JoinClause;
import com.octo.java.sql.query.visitor.QueryVisitor;

public class SelectQuery extends Query<SelectQuery> {
  public enum Order {
    DESC, ASC;
  }

  /**
   * Contains String or SQLFunc
   */
  protected final Object[] columns;
  protected String[] tables;
  private final List<JoinClause> joinClauses = new ArrayList<JoinClause>();
  private final Map<String, Order> orderBy = new LinkedHashMap<String, Order>();
  private Long limit = null;
  private String lastOrderByColumn = null;
  private boolean lastOrderByNeeded = true;
  private final List<SelectQuery> unions = new ArrayList<SelectQuery>();
  private String alias;

  /**
   * Constructor can only be called by factory methods in Query class
   * 
   * @param columns
   *          to put both SQLFunc & Colsname
   */
  SelectQuery(final Object... columns) {
    this.columns = columns;
  }

  public SelectQuery from(final String... newTables) {
    tables = newTables;
    return this;
  }

  public JoinClause innerJoin(final String table) {
    final JoinClause result = new JoinClause(table,
        JoinClause.JoinType.INNER_JOIN, this);
    joinClauses.add(result);
    return result;
  }

  public JoinClause leftOuterJoin(final String table) {
    final JoinClause result = new JoinClause(table,
        JoinClause.JoinType.LEFT_OUTER_JOIN, this);
    joinClauses.add(result);
    return result;
  }

  public SelectQuery orderBy(final String value) {
    this.orderBy.put(value, null);
    lastOrderByColumn = value;
    return this;
  }

  public SelectQuery orderBy(final String value, final boolean isOrderByNeeded) {
    lastOrderByNeeded = isOrderByNeeded;
    if (isOrderByNeeded) {
      return orderBy(value);
    }
    return this;
  }

  public SelectQuery desc() throws QueryGrammarException {
    if (!lastOrderByNeeded) {
      return this;
    }
    assertOrderBySpecified("desc");
    orderBy.put(lastOrderByColumn, Order.DESC);
    return this;
  }

  private void assertOrderBySpecified(final String operation)
      throws QueryGrammarException {
    if (lastOrderByColumn == null) {
      throw new QueryGrammarException("Cannot apply '" + operation
          + "' operator without order by column.");
    }
  }

  public SelectQuery asc() throws QueryGrammarException {
    if (!lastOrderByNeeded) {
      return this;
    }
    assertOrderBySpecified("asc");
    orderBy.put(lastOrderByColumn, Order.ASC);
    return this;
  }

  public SelectQuery limit(final Long newLimit) throws QueryGrammarException {
    this.limit = newLimit;
    return this;
  }

  public SelectQuery as(final String alias) {
    this.alias = alias;
    return this;
  }

  public SelectQuery union(final SelectQuery innerQuery) {
    unions.add(innerQuery);
    return this;
  }

  public void accept(final QueryVisitor visitor) {
    visitor.visit(this);
  }

  public Object[] getColumns() {
    return columns;
  }

  public String[] getTables() {
    return tables;
  }

  public List<JoinClause> getJoinClauses() {
    return joinClauses;
  }

  public String getAlias() {
    return alias;
  }

  public Map<String, Order> getOrderBy() {
    return orderBy;
  }

  public Long getLimit() {
    return limit;
  }

  public List<SelectQuery> getUnions() {
    return unions;
  }
}
