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

import org.apache.commons.collections.map.ListOrderedMap;

import com.octo.java.sql.query.visitor.QueryVisitor;

public class InsertQuery extends Query<InsertQuery> {
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

  public void accept(final QueryVisitor visitor) {
    visitor.visit(this);
  }

  public ListOrderedMap getColumnsValues() {
    return columnsValues;
  }

  public String getTable() {
    return table;
  }

}
