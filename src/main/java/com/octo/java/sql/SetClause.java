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

public class SetClause extends QueryPart {
  private final Exp exp;

  public SetClause(final String colName, final Object value) {
    super();
    exp = new ComparisonExp(colName, "=", value, false, true);
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result)
      throws QueryGrammarException {
    return this.exp.buildSQLQuery(result);
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    return this.exp.getParams(result);
  }

}
