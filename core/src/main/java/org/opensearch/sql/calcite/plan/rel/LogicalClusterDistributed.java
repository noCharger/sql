/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Logical form of the distributed {@code cluster} node. Converted to an OpenSearch physical
 * operator.
 */
public class LogicalClusterDistributed extends ClusterDistributed {

  public LogicalClusterDistributed(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      String sourceField,
      double threshold,
      String matchMode,
      String delims,
      int maxClusters,
      String labelField) {
    super(
        cluster, traits, input, sourceField, threshold, matchMode, delims, maxClusters, labelField);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LogicalClusterDistributed(
        getCluster(),
        traitSet,
        inputs.get(0),
        sourceField,
        threshold,
        matchMode,
        delims,
        maxClusters,
        labelField);
  }
}
