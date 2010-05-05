package com.octo.java.sql.query;

import static org.apache.commons.collections.CollectionUtils.exists;
import static org.apache.commons.lang.ArrayUtils.isEmpty;

import java.util.Arrays;

import org.apache.commons.collections.Predicate;

import com.octo.java.sql.exp.BetweenExp;
import com.octo.java.sql.exp.Exp;
import com.octo.java.sql.exp.ExpSeq;
import com.octo.java.sql.exp.InExp;
import com.octo.java.sql.exp.OpExp;
import com.octo.java.sql.query.visitor.DefaultVisitor;

public class BasicQueryOptimizer extends DefaultVisitor {
  @Override
  public void visit(final BetweenExp betweenExp) throws QueryException {
    super.visit(betweenExp);
    if ((betweenExp.getValueStart() == null)
        || (betweenExp.getValueEnd() == null))
      betweenExp.invalidate();
  }

  @Override
  public void visit(final InExp inExp) throws QueryException {
    super.visit(inExp);
    if (isEmpty(inExp.getValues()))
      inExp.invalidate();
    else {
      final boolean atLeastOneInValueIsNull = exists(Arrays.asList(inExp
          .getValues()), new Predicate() {
        public boolean evaluate(final Object object) {
          return object == null;
        }
      });
      if (atLeastOneInValueIsNull)
        inExp.invalidate();
    }
  }

  @Override
  public void visit(final OpExp exp) throws QueryException {
    super.visit(exp);
    if ((exp.getLhsValue() == null) || (exp.getRhsValue() == null))
      exp.invalidate();
  }

  @Override
  public void visit(final ExpSeq expSeq) throws QueryException {
    super.visit(expSeq);
    final boolean atLeastOneExpIsValid = exists(expSeq.getClauses(),
        new Predicate() {
          public boolean evaluate(final Object exp) {
            return ((Exp) exp).isValid();
          }
        });
    if (!atLeastOneExpIsValid)
      expSeq.invalidate();
  }
}
