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

import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SelectQuery extends Query<SelectQuery> {
  private static final String SELECT = "SELECT";
  public final static int DEFAULT_ROWSET_SIZE = 1000;

  private enum Order {
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

  private Integer minRowToFetch = null;
  private int rowSetSize = DEFAULT_ROWSET_SIZE;

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

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    result.append(SELECT).append(" ");
    for (int i = 0; i < columns.length; i++) {
      if (i > 0) {
        result.append(",");
      }
      final Object colItem = columns[i];
      if (colItem instanceof String) {
        result.append((String) colItem);
      } else if (colItem instanceof SQLFunc) {
        ((SQLFunc) colItem).buildSQLQuery(result);
      }
    }
    result.append(" FROM ");
    result.append(join(tables, ','));
    for (final JoinClause clause : joinClauses) {
      clause.buildSQLQuery(result);
    }
    if (alias != null) {
      result.append(" ");
      result.append(alias);
    }

    if ((whereClause != null) && (whereClause.isValid())) {
      result.append(" WHERE ");
      whereClause.buildSQLQuery(result);
    }

    int i = 1;
    final int orderBySize = orderBy.size();
    for (final String orderByColumn : orderBy.keySet()) {
      if (i == 1) {
        result.append(" ORDER BY ");
      }
      result.append(orderByColumn);
      final Order columnOrder = orderBy.get(orderByColumn);
      if (columnOrder != null) {
        result.append(" ").append(columnOrder.toString());
      }
      if (i < orderBySize) {
        result.append(", ");
      }
      ++i;
    }

    // if ((limit != null) && (minRowToFetch == null)) {
    if (limit != null) {
      if (oracleDialect) {
        result.insert(0, "SELECT * FROM (");
        result.append(") WHERE (rownum<=:limit)");
      } else {
        result.append(" LIMIT ").append(":limit");
      }
    }

    if (minRowToFetch != null) {
      if (oracleDialect) {
        result.insert(0, "" + "SELECT * FROM ("
            + "    SELECT  /*+ FIRST_ROWS(n) */ a.*, ROWNUM rnum FROM ("
            + "        SELECT * FROM (");

        result.append("" + "        )" + "    ) a WHERE (ROWNUM < :maxRowNum) "
            + ") WHERE (rnum >= :minRowNum)");
      } else {
        // throw new NotImplementedException();
      }
    }

    if (!isEmpty(unions)) {
      result.append(" UNION ");
      for (final SelectQuery union : unions) {
        union.buildSQLQuery(result);
      }
      result.append(" ");
    }

    return result;
  }

  @Override
  public Map<String, Object> getParams() {
    return getParams(new HashMap<String, Object>());
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    for (final Object obj : columns) {
      if (obj instanceof SQLFunc) {
        final SQLFunc func = (SQLFunc) obj;
        func.getParams(result);
      }
    }
    for (final JoinClause clause : joinClauses) {
      clause.getParams(result);
    }

    getWhereParams(result);

    if (limit != null) {
      result.put("limit", limit);
    }

    // Add params to load from row min to row max
    if (minRowToFetch != null) {
      result.put("minRowNum", minRowToFetch);
      result.put("maxRowNum", minRowToFetch + rowSetSize);
    }

    if (!isEmpty(unions)) {
      for (final SelectQuery union : unions) {
        union.getParams(result);
      }
    }

    return result;
  }

  public SelectQuery from(final String... newTables) {
    tables = newTables;
    return this;
  }

  public JoinClause leftInnerJoin(final String table) {
    final JoinClause result = new JoinClause(table,
        JoinClause.JoinType.LEFT_INNER_JOIN, this);
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

  /**
   * @param minRowToFetch
   *          the minRowToFetch to set. The minimum row num is always 1 in
   *          database.
   */
  public SelectQuery minRowToFetch(final Integer minRowToFetch) {
    this.minRowToFetch = minRowToFetch;
    return this;
  }

  public SelectQuery as(final String alias) {
    this.alias = alias;
    return this;
  }

  /**
   * It will fix the max number of rows to download during each call (if the
   * {@link #minRowToFetch(Integer)} is also set). By default
   * {@link #DEFAULT_ROWSET_SIZE} is used, but you can override that here.
   * 
   * @param rowSetSize
   *          the rowsetSize to set.
   */
  public SelectQuery rowSetSize(final int rowSetSize) {
    this.rowSetSize = rowSetSize;
    return this;
  }

  public SelectQuery union(final SelectQuery innerQuery) {
    unions.add(innerQuery);
    return this;
  }

}
