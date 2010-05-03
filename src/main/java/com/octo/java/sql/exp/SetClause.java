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

package com.octo.java.sql.exp;

import com.octo.java.sql.query.visitor.QueryVisitor;
import com.octo.java.sql.query.visitor.Visitable;

public class SetClause implements Visitable {
  private final Column column;
  private final Object value;

  public SetClause(final Column column, final Object value) {
    super();
    this.column = column;
    this.value = value;
  }

  public void accept(final QueryVisitor visitor) {
    visitor.visit(this);
  }

  public Column getColumn() {
    return column;
  }

  public Object getValue() {
    return value;
  }

}
