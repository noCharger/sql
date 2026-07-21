/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.sql.common.cluster.ClusterPartialState;
import org.opensearch.sql.common.cluster.MatchMode;
import org.opensearch.sql.common.cluster.TextSimilarityClustering;

/**
 * Map phase of the distributed {@code cluster} aggregation. On each shard it reads the source field
 * per document and runs the same greedy, threshold-based clustering the single-node command uses,
 * accumulating shard-local representatives. {@link #buildAggregation} emits an {@link
 * InternalClusterResult} carrying those representatives for the coordinating node to reduce.
 */
public class ClusterAggregator extends MetricsAggregator {

  private final String fieldName;
  private final double threshold;
  private final MatchMode matchMode;
  private final String delims;
  private final int maxClusters;
  private final TextSimilarityClustering similarity;
  private final Map<Long, ShardClusters> perBucket = new HashMap<>();

  public ClusterAggregator(
      String name,
      SearchContext context,
      Aggregator parent,
      Map<String, Object> metadata,
      String fieldName,
      double threshold,
      MatchMode matchMode,
      String delims,
      int maxClusters)
      throws IOException {
    super(name, context, parent, metadata);
    this.fieldName = fieldName;
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
    this.maxClusters = maxClusters;
    this.similarity = new TextSimilarityClustering(threshold, matchMode, delims);
  }

  @Override
  public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub)
      throws IOException {
    LeafSearchLookup leafLookup =
        context().getQueryShardContext().lookup().getLeafSearchLookup(ctx);
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        leafLookup.setDocument(doc);
        Object value = leafLookup.source().loadSourceIfNeeded().get(fieldName);
        if (value == null) {
          return;
        }
        perBucket
            .computeIfAbsent(owningBucketOrd, k -> new ShardClusters())
            .add(value.toString(), similarity, threshold, maxClusters);
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
    ShardClusters clusters = perBucket.get(owningBucketOrd);
    List<ClusterPartialState.Representative> reps =
        clusters == null ? List.of() : clusters.toRepresentatives();
    return new InternalClusterResult(
        name(), new ClusterPartialState(threshold, matchMode, delims, reps), metadata());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalClusterResult(
        name(), new ClusterPartialState(threshold, matchMode, delims, List.of()), metadata());
  }

  /**
   * Shard-local greedy clustering state: representative text plus member count, in arrival order.
   */
  private static final class ShardClusters {
    private final List<String> texts = new ArrayList<>();
    private final List<Long> counts = new ArrayList<>();

    void add(String value, TextSimilarityClustering sim, double threshold, int maxClusters) {
      int best = -1;
      double bestSim = 0.0;
      for (int i = 0; i < texts.size(); i++) {
        double s = sim.computeSimilarity(value, texts.get(i));
        if (s > bestSim) {
          bestSim = s;
          best = i;
        }
      }
      if (best >= 0 && bestSim >= threshold - 1e-9) {
        counts.set(best, counts.get(best) + 1);
      } else if (texts.size() < maxClusters) {
        texts.add(value);
        counts.add(1L);
      } else if (best >= 0) {
        // At the cluster cap: fold into the nearest existing cluster, matching the command's clamp.
        counts.set(best, counts.get(best) + 1);
      }
    }

    List<ClusterPartialState.Representative> toRepresentatives() {
      List<ClusterPartialState.Representative> reps = new ArrayList<>(texts.size());
      for (int i = 0; i < texts.size(); i++) {
        reps.add(new ClusterPartialState.Representative(texts.get(i), counts.get(i)));
      }
      return reps;
    }
  }
}
