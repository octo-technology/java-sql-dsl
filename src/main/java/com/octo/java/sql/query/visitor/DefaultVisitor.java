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

import org.apache.commons.collections.map.ListOrderedMap;

import com.octo.java.sql.exp.BetweenExp;
import com.octo.java.sql.exp.Column;
import com.octo.java.sql.exp.Constant;
import com.octo.java.sql.exp.Exp;
import com.octo.java.sql.exp.ExpSeq;
import com.octo.java.sql.exp.InExp;
import com.octo.java.sql.exp.JavaSQLFunc;
import com.octo.java.sql.exp.JoinClause;
import com.octo.java.sql.exp.Nullable;
import com.octo.java.sql.exp.OpExp;
import com.octo.java.sql.exp.SQLFunc;
import com.octo.java.sql.exp.SetClause;
import com.octo.java.sql.query.DeleteQuery;
import com.octo.java.sql.query.InsertQuery;
import com.octo.java.sql.query.SelectQuery;
import com.octo.java.sql.query.UpdateQuery;

public class DefaultVisitor extends BaseVisitor {

  public void visitValue(final Object value) {
  }

  public void visit(final SQLFunc func) {
    for (final Object param : func.getParams()) {
      acceptOrVisitValue(param);
    }
  }

  public void visit(final JavaSQLFunc javaSQLFunc) {

  }

  public void visit(final Column column) {
  }

  public void visit(final OpExp exp) {
    acceptOrVisitValue(exp.getLhsValue());
    acceptOrVisitValue(exp.getRhsValue());
  }

  public void visit(final BetweenExp betweenExp) {
    betweenExp.getColumn().accept(this);
    acceptOrVisitValue(betweenExp.getValueStart());
    acceptOrVisitValue(betweenExp.getValueEnd());
  }

  public void visit(final ExpSeq expSeq) {
    for (final Exp clause : expSeq.getClauses())
      clause.accept(this);
  }

  public void visit(final InExp inExp) {
    inExp.getColumn().accept(this);
    for (final Object value : inExp.getValues())
      acceptOrVisitValue(value);
  }

  public void visit(final JoinClause joinClause) {
    joinClause.getOnClause().accept(this);
  }

  public void visit(final SetClause setClause) {
    setClause.getColumn().accept(this);
    acceptOrVisitValue(setClause.getValue());
  }

  public void visit(final Constant constant) {
  }

  public void visit(final SelectQuery query) {
    for (final Object column : query.getColumns())
      acceptOrVisitValue(column);
    for (final JoinClause clause : query.getJoinClauses())
      clause.accept(this);
    final Exp whereClause = query.getWhereClause();
    if ((whereClause != null) && (whereClause.isValid()))
      whereClause.accept(this);
    for (final SelectQuery union : query.getUnions())
      union.accept(this);
  }

  public void visit(final UpdateQuery updateQuery) {
    for (final SetClause clause : updateQuery.getSetClauses())
      clause.accept(this);
    final Exp whereClause = updateQuery.getWhereClause();
    if ((whereClause != null) && (whereClause.isValid()))
      whereClause.accept(this);
  }

  public void visit(final InsertQuery insertQuery) {
    final ListOrderedMap columnValues = insertQuery.getColumnsValues();
    for (final Object column : columnValues.keyList())
      acceptOrVisitValue(columnValues.get(column));
  }

  public void visit(final DeleteQuery deleteQuery) {
    final Exp whereClause = deleteQuery.getWhereClause();
    if ((whereClause != null) && (whereClause.isValid()))
      whereClause.accept(this);
  }

  public void accept(final Nullable nullable) {
    if (!nullable.isNull())
      acceptOrVisitValue(nullable.getValue());
  }
}
