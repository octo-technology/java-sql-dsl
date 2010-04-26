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

import java.util.Map;

public class JoinClause extends QueryPart {
  private static final String ON = "ON";

  public enum JoinType {
    LEFT_OUTER_JOIN("LEFT OUTER JOIN"), RIGHT_OUTER_JOIN("RIGHT OUTER JOIN"), LEFT_INNER_JOIN(
        "LEFT INNER JOIN"), RIGHT_INNER_JOIN("RIGHT INNER JOIN");

    public final String value;

    JoinType(final String value) {
      this.value = value;
    }
  }

  private final String table;
  private final JoinType joinType;
  private Exp onClause;
  private final SelectQuery query;

  public JoinClause(final String table, final JoinType joinType,
      final SelectQuery query) {
    super();
    this.table = table;
    this.joinType = joinType;
    this.query = query;
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    result.append(" ").append(joinType.value).append(" ").append(table);
    result.append(" ").append(ON).append(" ");
    return onClause.buildSQLQuery(result);
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    return onClause.getParams(result);
  }

  public SelectQuery on(final String columnName, final String operator,
      final Object value, final boolean valueIsColumnName) {
    onClause = new ComparisonExp(columnName, operator, value,
        valueIsColumnName, false);
    return query;
  }

  public SelectQuery on(final Exp exp) {
    onClause = exp;
    return query;
  }

  public JoinClause on(final String columnName) {
    onClause = new ComparisonExp(columnName);
    return this;
  }

  public SelectQuery eq(final String value) throws QueryGrammarException {
    return eq(value, true);
  }

  public SelectQuery eq(final Object value, final boolean valueIsColumnName)
      throws QueryGrammarException {
    assertOnClauseIsInitialized("eq");
    onClause.eq(value, valueIsColumnName);
    return query;
  }

  private void assertOnClauseIsInitialized(final String operation)
      throws QueryGrammarException {
    if (onClause == null)
      throw new QueryGrammarException("Cannot apply '" + operation
          + "' operation without an initialized join clause.");
  }
}
