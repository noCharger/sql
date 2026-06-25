/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Command-agnostic schema-on-read source-include collector (RFC <a
 * href="https://github.com/opensearch-project/sql/issues/4984">#4984</a>, Step 5).
 *
 * <p>The schema-on-read {@code _MAP} catch-all column is materialized at read time from a hit's
 * {@code _source}. To avoid pulling the whole document, the scan can be told exactly which dynamic
 * field names the query consumes. Instead of an AST pre-pass that needs a hand-written visitor per
 * command, this collector runs once over the <b>built logical plan</b>, where every dynamic-field
 * reference has already been lowered to a uniform shape regardless of the command that produced it:
 *
 * <ul>
 *   <li>{@code ITEM(<map>, '<field>')} — a single unmapped field read (see {@link
 *       org.opensearch.sql.calcite.QualifiedNameResolver}); the literal key is the field name.
 *   <li>{@code MAP_FILTER_KEYS(<map>, '<pattern>')} — a partial-wildcard selection; the literal
 *       pattern (e.g. {@code result.user.*}) is added directly, as OpenSearch {@code _source}
 *       includes support {@code *} wildcards.
 * </ul>
 *
 * <p>Because the scan is reached at the RexNode layer (homogeneous) rather than the AST layer
 * (heterogeneous), <b>every command — current and future — is covered with no per-command code.</b>
 * The collected names are applied to every scan that carries a {@code _MAP} column via {@link
 * SchemaOnReadPushDown#applyDynamicFieldSourceIncludes(Set)}.
 *
 * <p><b>Over-collection is safe.</b> {@code _source} includes are additive: an extra name that is
 * not a real source field is simply ignored by OpenSearch, and never drops a needed field. The only
 * unsafe direction is under-collection, so matching is kept deliberately broad (any {@code ITEM} on
 * a {@code MAP}-typed operand). If nothing is collected, the request builder falls back to fetching
 * the full {@code _source}, so correctness is never at risk — only the optimization.
 */
@UtilityClass
public class DynamicFieldSourceCollector {

  /**
   * Collect dynamic-field references across {@code root} and push them to every {@code
   * _MAP}-bearing scan as targeted {@code _source} includes. Safe to call unconditionally; it does
   * nothing when there are no dynamic-field references.
   */
  public static void apply(RelNode root) {
    if (root == null) {
      return;
    }
    Set<String> includes = new HashSet<>();
    List<SchemaOnReadPushDown> scans = new ArrayList<>();
    RexReferenceCollector rexCollector = new RexReferenceCollector(includes);

    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        node.accept(rexCollector); // collect ITEM(_MAP, ...) / MAP_FILTER_KEYS(_MAP, ...) literals
        if (node instanceof SchemaOnReadPushDown) {
          scans.add((SchemaOnReadPushDown) node);
        }
        super.visit(node, ordinal, parent);
      }
    }.go(root);

    if (includes.isEmpty() || scans.isEmpty()) {
      return;
    }
    Set<String> immutable = Set.copyOf(includes);
    for (SchemaOnReadPushDown scan : scans) {
      scan.applyDynamicFieldSourceIncludes(immutable);
    }
  }

  /** RexShuttle that records dynamic-field literal keys/patterns; it never rewrites the tree. */
  private static final class RexReferenceCollector extends RexShuttle {
    private final Set<String> includes;

    RexReferenceCollector(Set<String> includes) {
      this.includes = includes;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      SqlOperator op = call.getOperator();
      List<RexNode> operands = call.getOperands();
      if (op == SqlStdOperatorTable.ITEM && operands.size() == 2 && isMap(operands.get(0))) {
        addLiteral(operands.get(1));
      } else if (op == PPLBuiltinOperators.MAP_FILTER_KEYS && operands.size() == 2) {
        addLiteral(operands.get(1));
      }
      return super.visitCall(call); // recurse into nested operands
    }

    private boolean isMap(RexNode node) {
      return node.getType().getSqlTypeName() == SqlTypeName.MAP;
    }

    private void addLiteral(RexNode node) {
      if (node instanceof RexLiteral
          && SqlTypeName.CHAR_TYPES.contains(node.getType().getSqlTypeName())) {
        String value = ((RexLiteral) node).getValueAs(String.class);
        if (value != null && !value.isEmpty()) {
          includes.add(value);
        }
      }
    }
  }
}
