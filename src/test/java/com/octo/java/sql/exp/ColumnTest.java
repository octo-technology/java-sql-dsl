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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ColumnTest {

  @Test
  public void testShouldGetTableNameFromColumnWithTableName() {
    assertEquals("table", new Column("table.col").getTableName());
  }

  @Test
  public void testShouldGetNullFromColumnWithoutTableName() {
    assertEquals(null, new Column("col").getTableName());
  }

  @Test
  public void testShouldGetNullFromColumnWithEmptyTableName() {
    assertEquals(null, new Column(".col").getTableName());
  }
}
