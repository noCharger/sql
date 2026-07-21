/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.cluster;

import java.io.IOException;
import java.util.Map;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.sql.common.cluster.MatchMode;

/** Builds a {@link ClusterAggregator} on each shard for the distributed {@code cluster} path. */
public class ClusterAggregatorFactory extends AggregatorFactory {

  private final String fieldName;
  private final double threshold;
  private final MatchMode matchMode;
  private final String delims;
  private final int maxClusters;

  public ClusterAggregatorFactory(
      String name,
      String fieldName,
      double threshold,
      MatchMode matchMode,
      String delims,
      int maxClusters,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder,
      Map<String, Object> metadata)
      throws IOException {
    super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
    this.fieldName = fieldName;
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
    this.maxClusters = maxClusters;
  }

  @Override
  protected Aggregator createInternal(
      SearchContext searchContext,
      Aggregator parent,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata)
      throws IOException {
    return new ClusterAggregator(
        name,
        searchContext,
        parent,
        metadata,
        fieldName,
        threshold,
        matchMode,
        delims,
        maxClusters);
  }
}
