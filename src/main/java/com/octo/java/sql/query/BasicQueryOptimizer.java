package com.octo.java.sql.query;

import static org.apache.commons.collections.CollectionUtils.exists;
import static org.apache.commons.lang.ArrayUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.isEmpty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import com.octo.java.sql.exp.BetweenExp;
import com.octo.java.sql.exp.Column;
import com.octo.java.sql.exp.Constant;
import com.octo.java.sql.exp.Exp;
import com.octo.java.sql.exp.ExpSeq;
import com.octo.java.sql.exp.InExp;
import com.octo.java.sql.exp.JoinClause;
import com.octo.java.sql.exp.OpExp;
import com.octo.java.sql.query.visitor.DefaultVisitor;

public class BasicQueryOptimizer extends DefaultVisitor {
  private final Set<String> usedTables = new HashSet<String>();
  private final Map<String, Set<String>> tableReverseDependency = new HashMap<String, Set<String>>();
  private final Map<String, JoinClause> tableJoin = new HashMap<String, JoinClause>();
  private String currentJoinedTable;

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

  @Override
  public void visit(final Column column) {
    super.visit(column);
    final String tableName = column.getTableName();
    if (!isEmpty(tableName)) {
      if (currentJoinedTable == null)
        usedTables.add(tableName);
      else if (!currentJoinedTable.equals(tableName))
        addTableReverseDependency(currentJoinedTable, tableName);
    }
  }

  @Override
  public void visit(final JoinClause joinClause) throws QueryException {
    currentJoinedTable = joinClause.getTable();
    super.visit(joinClause);
    tableJoin.put(currentJoinedTable, joinClause);
    currentJoinedTable = null;
  }

  @Override
  public void visit(final SelectQuery query) throws QueryException {
    super.visit(query);
    for (final String table : tableJoin.keySet())
      if (!isJoinNecessary(table))
        tableJoin.get(table).invalidate();
  }

  @Override
  public void visit(final Constant constant) {
    super.visit(constant);
    if (Constant.STAR.equals(constant))
      usedTables.add(null);
  }

  /**
   * Add table1 as a reverse dependency of table2 (ie. table1 depends on table2)
   * 
   * @param table1
   * @param table2
   */
  private void addTableReverseDependency(final String table1,
      final String table2) {
    Set<String> reverseDependencies;
    if (!tableReverseDependency.containsKey(table2)) {
      reverseDependencies = new HashSet<String>();
      tableReverseDependency.put(table2, reverseDependencies);
    } else
      reverseDependencies = tableReverseDependency.get(table2);
    reverseDependencies.add(table1);
  }

  /**
   * Determine if given joined table is used. <br>
   * 4 possibles cases : <br>
   * - null is in usedTables which means a column name has been specified
   * without the table it belongs to<br>
   * - '*' is in usedTables<br>
   * - a column of the table has been used in SELECT or in a WHERE clause<br>
   * - another table which is necessary has a dependency on the given table<br>
   * In this exemple, "table1" depends on "table2", "table3" depends on "table1"
   * and join on "table1" is necessary because "table3" is necessary because a
   * "table3" column is used in SELECT:<br>
   * <code>
   * query = select(c("table3.column")).from("table2");
   * query.innerJoin("table1").on(c("table1.column")).eq(c("table2.column"));
   * query.innerJoin("table3").on(c("table3.column")).eq(c("table1.column"));
   * </code>
   * 
   * @param table
   * @return
   */
  protected boolean isJoinNecessary(final String table) {
    return usedTables.contains(null)
        || usedTables.contains(Constant.STAR.getValue())
        || usedTables.contains(table) //
        || (tableReverseDependency.containsKey(table) && CollectionUtils
            .exists(tableReverseDependency.get(table), new Predicate() {
              public boolean evaluate(final Object dependency) {
                return isJoinNecessary((String) dependency);
              }
            }));
  }
}
