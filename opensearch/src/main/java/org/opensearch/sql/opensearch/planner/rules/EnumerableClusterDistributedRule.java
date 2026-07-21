/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.calcite.plan.rel.LogicalClusterDistributed;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableClusterDistributed;

/** Converts a {@link LogicalClusterDistributed} to the OpenSearch physical operator. */
public class EnumerableClusterDistributedRule extends ConverterRule {

  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              LogicalClusterDistributed.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableClusterDistributedRule")
          .withRuleFactory(EnumerableClusterDistributedRule::new);

  protected EnumerableClusterDistributedRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalClusterDistributed node = (LogicalClusterDistributed) rel;
    var traitSet = node.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode convertedInput =
        convert(
            node.getInput(), node.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE));
    return new CalciteEnumerableClusterDistributed(
        node.getCluster(),
        traitSet,
        convertedInput,
        node.getSourceField(),
        node.getThreshold(),
        node.getMatchMode(),
        node.getDelims(),
        node.getMaxClusters(),
        node.getLabelField());
  }
}
