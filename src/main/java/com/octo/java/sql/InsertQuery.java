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

import static org.apache.commons.lang.StringUtils.join;

import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.apache.log4j.Logger;

public class InsertQuery extends Query<InsertQuery> {
  /**
   * Logger for this class
   */
  private static final Logger logger = Logger.getLogger(InsertQuery.class);

  private static final String INSERT = "INSERT INTO";

  private final ListOrderedMap columnsValues = new ListOrderedMap();

  private final String table;

  /**
   * Constructor
   * 
   * @param table
   */
  InsertQuery(final String table) {
    this.table = table;
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result) {
    result.append(INSERT).append(" ");
    result.append(table);
    result.append(" (");
    result.append(join(columnsValues.keyList(), ", "));
    result.append(") VALUES (");
    boolean firstClause = true;
    for (final Object column : columnsValues.keyList()) {
      if (firstClause) {
        firstClause = false;
      } else {
        result.append(", ");
      }
      result.append(":").append(column);
    }
    result.append(")");

    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> getParams() {
    if (logger.isDebugEnabled()) {
      logger
          .debug("getParams() - ListOrderedMap columnsValues=" + columnsValues); //$NON-NLS-1$
    }
    return columnsValues;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    result.putAll(columnsValues);
    return result;
  }

  public InsertQuery set(final String column, final Object value)
      throws QueryGrammarException {
    if (columnsValues.containsKey(column))
      throw new QueryGrammarException("Column '" + column
          + "' has already been set.");
    columnsValues.put(column, value);
    return this;
  }

  public InsertQuery set(final String column, final Object value,
      final Object defaultValueIfNull) throws QueryGrammarException {
    if (value == null)
      return set(column, defaultValueIfNull);
    else
      return set(column, value);
  }

}
