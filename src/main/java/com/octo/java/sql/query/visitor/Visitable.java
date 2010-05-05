package com.octo.java.sql.query.visitor;

import com.octo.java.sql.query.QueryException;

public interface Visitable {
  public void accept(final QueryVisitor visitor) throws QueryException;
}
