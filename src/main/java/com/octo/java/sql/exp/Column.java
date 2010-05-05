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

public class Column implements Visitable {
  private final String name;

  public Column(final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof Column))
      return false;

    final Column otherObj = (Column) obj;
    if (name == null)
      return otherObj.getName() == null;
    else
      return name.equals(otherObj.getName());
  }

  @Override
  public String toString() {
    return name;
  }

  public void accept(final QueryVisitor visitor) {
    visitor.visit(this);
  }

  public String getTableName() {
    if (name.indexOf(".") > 0)
      return name.split("\\.")[0];
    else
      return null;
  }
}
