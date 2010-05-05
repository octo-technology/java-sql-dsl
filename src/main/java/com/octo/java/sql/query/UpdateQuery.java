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
import java.util.List;

import com.octo.java.sql.exp.Column;
import com.octo.java.sql.exp.SetClause;
import com.octo.java.sql.query.visitor.QueryVisitor;

public class UpdateQuery extends Query<UpdateQuery> {
  private final List<SetClause> setClauses = new ArrayList<SetClause>();
  private final String table;

  /**
   * Constructor can only be called by factory methods in Query class
   * 
   * @param columns
   */
  UpdateQuery(final String table) {
    this.table = table;
  }

  public UpdateQuery set(final Column column, final Object value) {
    setClauses.add(new SetClause(column, value));
    return this;
  }

  public void accept(final QueryVisitor visitor) throws QueryException {
    visitor.visit(this);
  }

  public String getTable() {
    return table;
  }

  public List<SetClause> getSetClauses() {
    return setClauses;
  }
}
