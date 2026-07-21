/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Abstract RelNode for the distributed (map/reduce) {@code cluster} path. It has one input (the
 * rows to label) and appends a {@code cluster_label} column, so its output row type is the input
 * row type plus one integer label field.
 *
 * <p>This is a storage-agnostic logical node. The OpenSearch physical implementation runs two
 * phases at execution time: phase one issues the {@code ppl_cluster} aggregation to build the
 * global model, phase two labels each row against that model. Per-row output is approximate versus
 * a single global pass, so this path is opt-in.
 */
@Getter
public abstract class ClusterDistributed extends SingleRel {

  protected final String sourceField;
  protected final double threshold;
  protected final String matchMode;
  protected final String delims;
  protected final int maxClusters;
  protected final String labelField;

  protected ClusterDistributed(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      String sourceField,
      double threshold,
      String matchMode,
      String delims,
      int maxClusters,
      String labelField) {
    super(cluster, traits, input);
    this.sourceField = sourceField;
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
    this.maxClusters = maxClusters;
    this.labelField = labelField;
  }

  @Override
  protected RelDataType deriveRowType() {
    RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    for (var field : getInput().getRowType().getFieldList()) {
      builder.add(field);
    }
    RelDataType labelType = getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    builder.add(labelField, labelType);
    return builder.build();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("sourceField", sourceField)
        .item("threshold", threshold)
        .item("matchMode", matchMode)
        .item("delims", delims)
        .item("maxClusters", maxClusters)
        .item("labelField", labelField);
  }

  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);
}
