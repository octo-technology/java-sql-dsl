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

import com.octo.java.sql.query.SelectQuery;

public class OracleQueryBuilder extends DefaultQueryBuilder {
  @Override
  protected void buildLimitClause(final SelectQuery query) {
    result.insert(0, "SELECT * FROM (");
    result.append(") WHERE (rownum<=:");
    result.append(addVariable(query.getLimit(), "limit")).append(")");
  }
}
