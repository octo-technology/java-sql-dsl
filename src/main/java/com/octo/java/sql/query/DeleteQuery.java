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

import com.octo.java.sql.query.visitor.QueryVisitor;

public class DeleteQuery extends Query<DeleteQuery> {
  private final String[] tables;

  /**
   * Constructor can only be called by factory methods in Query class
   * 
   * @param tables
   */
  DeleteQuery(final String... tables) {
    this.tables = tables;
  }

  public void accept(final QueryVisitor visitor) throws QueryException {
    visitor.visit(this);
  }

  public String[] getTables() {
    return tables;
  }
}
