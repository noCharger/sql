/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import java.math.BigDecimal;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.LogicalCluster;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Planner rule that converts a {@link LogicalCluster} into the equivalent in-process window plan,
 * i.e.
 *
 * <pre>
 * | cluster ENAME
 *
 * LogicalCluster(source=[$1], ...)
 *
 * becomes:
 *
 * LogicalProject(..., cluster_label=[$8])
 * +- LogicalFilter(condition=[=(_cluster_convergence_row_num, 1)])     (only when !labelOnly)
 *    +- LogicalProject(..., _cluster_convergence_row_num=[ROW_NUMBER() OVER (PARTITION BY $8)])
 *       +- LogicalProject(..., cluster_label=[ITEM($8, CAST(ROW_NUMBER() OVER ()):INTEGER)])
 *          +- LogicalProject(..., _cluster_labels_array=[cluster_label($1, ...) OVER ()])
 *             +- &lt;input&gt;
 * </pre>
 *
 * <p>{@link #buildClusterWindowPlan} is the single source of truth for the window lowering. The
 * rule matches {@code anyInputs}, so it fires whether or not the OpenSearch scan-pushdown rule
 * matched; the cost-based planner prefers the pushed-down scan when available and falls back to this
 * window plan otherwise.
 */
@Value.Enclosing
public class PPLClusterConvertRule extends RelRule<PPLClusterConvertRule.Config> {

  protected PPLClusterConvertRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalCluster cluster = call.rel(0);
    RelBuilder relBuilder = call.builder();
    relBuilder.push(cluster.getInput());
    buildClusterWindowPlan(
        relBuilder,
        cluster.getSourceField(),
        cluster.getThreshold(),
        cluster.getMatchMode(),
        cluster.getDelims(),
        cluster.getBufferLimit(),
        cluster.getMaxClusters(),
        cluster.getLabelField(),
        cluster.getCountField(),
        cluster.isShowCount(),
        cluster.isLabelOnly());
    call.transformTo(relBuilder.build());
  }

  /**
   * Build the buffered-window clustering plan on top of the current top of {@code relBuilder}. The
   * input is expected to already carry any {@code IS NOT NULL} filter on the source field. This is
   * the shared lowering used by {@link #onMatch}; it mirrors the semantics of the greedy clustering
   * window function ({@code cluster_label} UDAF) that buffers all rows.
   */
  public static void buildClusterWindowPlan(
      RelBuilder relBuilder,
      RexNode sourceField,
      double threshold,
      String matchMode,
      String delims,
      int bufferLimit,
      int maxClusters,
      String labelField,
      String countField,
      boolean showCount,
      boolean labelOnly) {
    // Resolve clustering as a window function over all rows (unbounded frame). The window function
    // buffers all rows, runs the greedy clustering algorithm, and returns an array of cluster
    // labels (one per input row, in order). The frame MUST be UNBOUNDED PRECEDING .. UNBOUNDED
    // FOLLOWING so every row sees the full label array before ITEM-indexing by ROW_NUMBER.
    RexNode clusterWindow =
        relBuilder
            .aggregateCall(
                PPLBuiltinOperators.CLUSTER_LABEL,
                sourceField,
                relBuilder.literal(threshold),
                relBuilder.literal(matchMode),
                relBuilder.literal(delims),
                relBuilder.literal(bufferLimit),
                relBuilder.literal(maxClusters))
            .over()
            .partitionBy()
            .orderBy()
            .rowsBetween(RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING)
            .toRex();
    String arrayAlias = "_cluster_labels_array";
    relBuilder.projectPlus(relBuilder.alias(clusterWindow, arrayAlias));

    // Add ROW_NUMBER to index into the array (1-based).
    String rowNumAlias = "_cluster_row_idx";
    RexNode rowNum =
        relBuilder
            .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
            .over()
            .rowsBetween(RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.CURRENT_ROW)
            .as(rowNumAlias);
    relBuilder.projectPlus(rowNum);

    // Extract the label for this row: array[row_number] (ITEM access is 1-based).
    RexNode rowIdxAsInt =
        relBuilder
            .getRexBuilder()
            .makeCast(
                relBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
                relBuilder.field(rowNumAlias));
    RexNode labelExpr =
        relBuilder
            .getRexBuilder()
            .makeCall(SqlStdOperatorTable.ITEM, relBuilder.field(arrayAlias), rowIdxAsInt);
    relBuilder.projectPlus(relBuilder.alias(labelExpr, labelField));

    // Remove the temporary array and row index columns.
    relBuilder.projectExcept(relBuilder.field(arrayAlias), relBuilder.field(rowNumAlias));

    if (showCount) {
      // cluster_count = COUNT(*) OVER (PARTITION BY cluster_label)
      RexNode countWindow =
          relBuilder
              .aggregateCall(SqlStdOperatorTable.COUNT)
              .over()
              .partitionBy(relBuilder.field(labelField))
              .rowsBetween(
                  RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING)
              .as(countField);
      relBuilder.projectPlus(countWindow);
    }

    if (!labelOnly) {
      // Filter to representative rows only: keep the first event per cluster.
      String convergenceRowNum = "_cluster_convergence_row_num";
      RexNode convergenceRn =
          relBuilder
              .aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
              .over()
              .partitionBy(relBuilder.field(labelField))
              .rowsTo(RexWindowBounds.CURRENT_ROW)
              .as(convergenceRowNum);
      relBuilder.projectPlus(convergenceRn);
      relBuilder.filter(
          relBuilder
              .getRexBuilder()
              .makeCall(
                  SqlStdOperatorTable.EQUALS,
                  relBuilder.field(convergenceRowNum),
                  relBuilder.getRexBuilder().makeExactLiteral(BigDecimal.ONE)));
      relBuilder.projectExcept(relBuilder.field(convergenceRowNum));
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config CLUSTER_CONVERTER =
        ImmutablePPLClusterConvertRule.Config.builder()
            .build()
            .withOperandSupplier(b0 -> b0.operand(LogicalCluster.class).anyInputs());

    @Override
    default PPLClusterConvertRule toRule() {
      return new PPLClusterConvertRule(this);
    }
  }

  public static final PPLClusterConvertRule CLUSTER_CONVERT_RULE =
      PPLClusterConvertRule.Config.CLUSTER_CONVERTER.toRule();
}
