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
import com.octo.java.sql.query.SelectQuery;
import com.octo.java.sql.query.visitor.QueryVisitor;
import com.octo.java.sql.query.visitor.Visitable;

public class JoinClause implements Visitable {
  public static enum JoinType {
    LEFT_OUTER_JOIN("LEFT OUTER JOIN"), RIGHT_OUTER_JOIN("RIGHT OUTER JOIN"), INNER_JOIN(
        "INNER JOIN");

    public final String value;

    JoinType(final String value) {
      this.value = value;
    }
  }

  private final String table;
  private final JoinType joinType;
  private Exp onClause;
  private final SelectQuery query;
  private boolean valid = true;

  public JoinClause(final String table, final JoinType joinType,
      final SelectQuery query) {
    super();
    this.table = table;
    this.joinType = joinType;
    this.query = query;
  }

  public SelectQuery on(final Column column, final Operator operator,
      final Object value) {
    onClause = new OpExp(column, operator, value);
    return query;
  }

  public SelectQuery on(final Exp exp) {
    onClause = exp;
    return query;
  }

  public JoinClause on(final Column column) {
    onClause = new OpExp(column);
    return this;
  }

  public SelectQuery eq(final Object value) throws QueryGrammarException {
    assertOnClauseIsInitialized("eq");
    onClause = onClause.eq(value);
    return query;
  }

  private void assertOnClauseIsInitialized(final String operation)
      throws QueryGrammarException {
    if (onClause == null)
      throw new QueryGrammarException("Cannot apply '" + operation
          + "' operation without an initialized join clause.");
  }

  public void accept(final QueryVisitor visitor) throws QueryException {
    visitor.visit(this);
  }

  public Exp getOnClause() {
    return onClause;
  }

  public JoinType getType() {
    return joinType;
  }

  public String getTable() {
    return table;
  }

  public boolean isValid() {
    return valid;
  }

  public void invalidate() {
    valid = false;
  }
}
