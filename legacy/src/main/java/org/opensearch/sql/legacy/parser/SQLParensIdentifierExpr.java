/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

/**
 * An Identifier that is wrapped in parentheses. This is for tracking in group bys the difference
 * between "group by state, age" and "group by (state), (age)". For non group by identifiers, it
 * acts as a normal SQLIdentifierExpr.
 */
public class SQLParensIdentifierExpr extends SQLExprImpl {

  private SQLIdentifierExpr expr;

  public SQLParensIdentifierExpr(SQLIdentifierExpr expr) {
    this.expr = new SQLIdentifierExpr(expr.getName());
  }

  public SQLIdentifierExpr getExpr() {
    return expr;
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SQLExpr clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void accept0(SQLASTVisitor visitor) {
    throw new UnsupportedOperationException();
  }
}
