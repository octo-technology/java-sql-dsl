package com.octo.java.sql.query.visitor;

public interface Visitable {
  public void accept(final QueryVisitor visitor);
}
