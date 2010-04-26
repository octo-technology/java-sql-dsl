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

import static org.apache.commons.collections.CollectionUtils.collect;
import static org.apache.commons.lang.StringUtils.join;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class CallQuery extends Query<CallQuery> {
  /**
   * Logger for this class
   */
  private static final Logger logger = Logger.getLogger(CallQuery.class);

  private static final String BEGIN = "BEGIN";
  private static final String END = "END";

  private final String procedure;

  private final Map<String, Object> parameters = new HashMap<String, Object>();

  /**
   * Constructor
   *
   * @param procedure
   */
  CallQuery(final String procedure) {
    this.procedure = procedure;
  }

  /**
   * Constructor
   *
   * @param procedure
   * @param params
   */
  CallQuery(final String procedure, final Object[] params) {
    this.procedure = procedure;
    for (int i = 0; i < params.length; i++) {
      parameters.put("param" + i, params[i]);
    }
  }

  @Override
  public StringBuilder buildSQLQuery(final StringBuilder result) {
    result.append(BEGIN).append(" ");
    result.append(procedure);
    result.append("(");
    result.append(join(collect(parameters.keySet(),
        QueryPart.variableTransformer), ","));
    result.append(");");
    result.append(END).append(";");
    return result;
  }

  @Override
  public Map<String, Object> getParams() {
    if (logger.isDebugEnabled()) {
      logger.debug("getParams() - Map<String,Object> result=" + parameters); //$NON-NLS-1$
    }
    return parameters;
  }

  @Override
  public Map<String, Object> getParams(final Map<String, Object> result) {
    result.putAll(parameters);
    return result;
  }

}
