/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import static org.opensearch.sql.calcite.plan.rule.PPLClusterConvertRule.CLUSTER_CONVERT_RULE;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import lombok.Getter;

/**
 * Relational expression representing the PPL {@code cluster} command.
 *
 * <p>This is a coordinator-side logical operator. Two rules give it a physical lowering:
 *
 * <ul>
 *   <li>{@link org.opensearch.sql.calcite.plan.rule.PPLClusterConvertRule} (registered from {@link
 *       #register}) rewrites it into the in-process window plan (buffered greedy clustering as a
 *       window function). This is the always-legal fallback and matches {@code anyInputs}.
 *   <li>The OpenSearch {@code ClusterIndexScanRule} pushes it into the index scan as a {@code
 *       scripted_metric} aggregation (distributed Map + coordinator Reduce) when the input is a raw
 *       pushable scan. This lives in the opensearch module.
 * </ul>
 *
 * <p>{@link #deriveRowType()} appends a {@code cluster_label INTEGER} column, and (only when {@code
 * showCount}) a {@code cluster_count BIGINT} column, after all input fields. This MUST byte-match
 * the output row type produced by the convert rule's window plan so {@code
 * RelOptRuleCall#transformTo} type-equivalence holds.
 */
@Getter
public class LogicalCluster extends SingleRel {

  private final RexNode sourceField;
  private final String sourceFieldName;
  private final double threshold;
  private final String matchMode;
  private final String delims;
  private final String labelField;
  private final String countField;
  private final boolean showCount;
  private final boolean labelOnly;
  private final int bufferLimit;
  private final int maxClusters;

  protected LogicalCluster(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RexNode sourceField,
      String sourceFieldName,
      double threshold,
      String matchMode,
      String delims,
      String labelField,
      String countField,
      boolean showCount,
      boolean labelOnly,
      int bufferLimit,
      int maxClusters) {
    super(cluster, traitSet, input);
    this.sourceField = sourceField;
    this.sourceFieldName = sourceFieldName;
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
    this.labelField = labelField;
    this.countField = countField;
    this.showCount = showCount;
    this.labelOnly = labelOnly;
    this.bufferLimit = bufferLimit;
    this.maxClusters = maxClusters;
  }

  public static LogicalCluster create(
      RelNode input,
      RexNode sourceField,
      String sourceFieldName,
      double threshold,
      String matchMode,
      String delims,
      String labelField,
      String countField,
      boolean showCount,
      boolean labelOnly,
      int bufferLimit,
      int maxClusters) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalCluster(
        cluster,
        traitSet,
        input,
        sourceField,
        sourceFieldName,
        threshold,
        matchMode,
        delims,
        labelField,
        countField,
        showCount,
        labelOnly,
        bufferLimit,
        maxClusters);
  }

  @Override
  protected RelDataType deriveRowType() {
    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.addAll(getInput().getRowType().getFieldList());
    // cluster_label = ITEM(array<int>, rowNum) -> nullable INTEGER (matches the window plan).
    builder.add(
        labelField,
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true));
    if (showCount) {
      // cluster_count = COUNT(*) OVER (...) -> BIGINT NOT NULL.
      builder.add(countField, typeFactory.createSqlType(SqlTypeName.BIGINT));
    }
    return builder.build();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalCluster(
        getCluster(),
        traitSet,
        sole(inputs),
        sourceField,
        sourceFieldName,
        threshold,
        matchMode,
        delims,
        labelField,
        countField,
        showCount,
        labelOnly,
        bufferLimit,
        maxClusters);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("source", sourceField)
        .item("threshold", threshold)
        .item("match", matchMode)
        .item("delims", delims)
        .item("labelField", labelField)
        .itemIf("countField", countField, showCount)
        .item("showCount", showCount)
        .item("labelOnly", labelOnly);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(CLUSTER_CONVERT_RULE);
  }
}
