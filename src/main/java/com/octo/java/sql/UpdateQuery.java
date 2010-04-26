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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.octo.java.sql.QueryPart.Operator;

public class UpdateQuery extends Query<UpdateQuery> {
  /**
   * Logger for this class
   */
  private static final Logger logger = Logger.getLogger(UpdateQuery.class);

  private static final String UPDATE = "UPDATE";
  private final List<ComparisonExp> setClauses = new ArrayList<ComparisonExp>();
  private final String table;

  /**
   * Constructor can only be called by factory methods in Query class
   * 
   * @param columns
   */
  UpdateQuery(final String table) {
    this.table = table;
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    result.append(UPDATE).append(" ");
    result.append(table);
    result.append(" SET ");
    boolean firstClause = true;
    for (final ComparisonExp clause : setClauses) {
      if (firstClause) {
        firstClause = false;
      } else {
        result.append(", ");
      }
      clause.buildSQLQuery(result);
    }

    buildWhereClause(result);

    return result;
  }

  @Override
  public Map<String, Object> getParams() {
    final Map<String, Object> params = getParams(new HashMap<String, Object>());
    if (logger.isInfoEnabled()) {
      logger.info("getParams() - Map<String,Object> params=" + params); //$NON-NLS-1$
    }
    return params;
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    for (final ComparisonExp clause : setClauses) {
      clause.getParams(result);
    }

    getWhereParams(result);
    return result;
  }

  public UpdateQuery set(final String column, final Object value) {
    setClauses.add(new ComparisonExp(column, Operator.EQ, value, false, true));
    return this;
  }

}
