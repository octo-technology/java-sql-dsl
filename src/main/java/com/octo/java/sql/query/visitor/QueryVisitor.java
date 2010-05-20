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

import com.octo.java.sql.exp.BetweenExp;
import com.octo.java.sql.exp.Column;
import com.octo.java.sql.exp.Constant;
import com.octo.java.sql.exp.ExpSeq;
import com.octo.java.sql.exp.InExp;
import com.octo.java.sql.exp.JoinClause;
import com.octo.java.sql.exp.Nullable;
import com.octo.java.sql.exp.OpExp;
import com.octo.java.sql.exp.SQLFunc;
import com.octo.java.sql.exp.SetClause;
import com.octo.java.sql.query.DeleteQuery;
import com.octo.java.sql.query.InsertQuery;
import com.octo.java.sql.query.QueryException;
import com.octo.java.sql.query.SelectQuery;
import com.octo.java.sql.query.UpdateQuery;

public interface QueryVisitor {
  public void visit(Column column);

  public void visit(OpExp exp) throws QueryException;

  public void visit(BetweenExp betweenExp) throws QueryException;

  public void visitValue(Object value);

  public void visit(ExpSeq expSeq) throws QueryException;

  public void visit(InExp inExp) throws QueryException;

  public void visit(JoinClause joinClause) throws QueryException;

  public void visit(SetClause setClause) throws QueryException;

  public void visit(SQLFunc sqlFunc) throws QueryException;

  public void visit(Constant constant);

  public void visit(SelectQuery query) throws QueryException;

  public void visit(UpdateQuery updateQuery) throws QueryException;

  public void visit(InsertQuery insertQuery) throws QueryException;

  public void visit(DeleteQuery deleteQuery) throws QueryException;

  public void visit(Nullable nullable) throws QueryException;
}
