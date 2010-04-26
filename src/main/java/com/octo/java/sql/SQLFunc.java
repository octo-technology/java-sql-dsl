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

/**
 * 
 */
package com.octo.java.sql;

import java.util.HashMap;
import java.util.Map;

public class SQLFunc extends QueryPart {
  protected final String funcName;
  protected Object[] params;
  private Map<String, Object> paramsMap;
  protected String alias;

  protected SQLFunc(final String funcName) {
    this.funcName = funcName;
  }

  SQLFunc(final String funcName, final Object... params) {
    this.funcName = funcName;
    this.params = params;
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result) {
    paramsMap = new HashMap<String, Object>();
    result.append(funcName).append("(");
    if (params != null) {
      for (int i = 0; i < params.length; ++i) {
        if (params[i] instanceof Column) {
          result.append(((Column) params[i]).getName());
        } else {
          final String variable = getVariableName(funcName);
          result.append(variableTransformer.transform(variable));
          paramsMap.put(variable, params[i]);
        }
        if (i < params.length - 1) {
          result.append(",");
        }
      }
    }
    result.append(")");
    if (alias != null)
      result.append(" AS ").append(alias);
    return result;
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    result.putAll(paramsMap);
    return result;
  }

  public String getName() {
    return funcName;
  }

  public SQLFunc as(final String alias) {
    this.alias = alias;
    return this;
  }
}
