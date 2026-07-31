/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.LogicalCluster;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that pushes a {@link LogicalCluster} down into a {@link CalciteLogicalIndexScan} as a
 * {@code scripted_metric} aggregation (the distributed cluster command Map phase). When the scan
 * cannot absorb the cluster (e.g. showcount/labelonly, or an eval-derived source field),
 * {@link CalciteLogicalIndexScan#pushDownCluster} returns null and the {@link
 * org.opensearch.sql.calcite.plan.rule.PPLClusterConvertRule} in-process window plan is used
 * instead.
 */
@Value.Enclosing
public class ClusterIndexScanRule extends InterruptibleRelRule<ClusterIndexScanRule.Config> {

  protected ClusterIndexScanRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final LogicalCluster cluster = call.rel(0);
    final CalciteLogicalIndexScan scan = call.rel(1);
    AbstractRelNode newRelNode = scan.pushDownCluster(cluster);
    if (newRelNode != null) {
      call.transformTo(newRelNode);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config DEFAULT =
        ImmutableClusterIndexScanRule.Config.builder()
            .build()
            .withDescription("Cluster-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalCluster.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(AbstractCalciteIndexScan::isLimitPushed)
                                            .and(AbstractCalciteIndexScan::noAggregatePushed))
                                    .noInputs()));

    @Override
    default ClusterIndexScanRule toRule() {
      return new ClusterIndexScanRule(this);
    }
  }
}
