/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Coordinator Reduce stage of the distributed cluster command.
 *
 * <p>Receives the {@link LocalClusterSummary} objects produced by every shard's Map stage and
 * applies the same greedy leader-clustering algorithm to their representatives, so the global
 * result is one representative row per global cluster.
 *
 * <p>Determinism rules (see the distributed cluster RFC):
 *
 * <ul>
 *   <li>Summaries are processed in the canonical order {@code (shardOrdinal, localCreationOrdinal)},
 *       never in network-arrival order.
 *   <li>A representative that meets the threshold against an existing global cluster joins it; ties
 *       resolve to the earliest-created cluster (strict {@code >} keeps the incumbent).
 *   <li>A representative is never updated once its cluster is created.
 *   <li>Global labels are assigned in creation order: 1, 2, 3, ...
 * </ul>
 *
 * <p>Similarity is computed through {@link TextSimilarityClustering} so the coordinator uses exactly
 * the same vectorization and cosine as the shards, keeping a single source of truth.
 */
public final class HierarchicalClusterMerge {

  /** Matches the epsilon used by the shard-local greedy assignment. */
  private static final double THRESHOLD_EPSILON = 1e-9;

  private HierarchicalClusterMerge() {}

  /** One merged global cluster: its 1-based label, representative output row, and total size. */
  public record GlobalCluster(int label, Map<String, Object> representativeRow, long count) {}

  /**
   * Merge shard-local summaries into global clusters.
   *
   * @param summaries local summaries from all shards (any order; sorted here)
   * @param threshold similarity threshold in (0, 1)
   * @param matchMode feature-extraction mode
   * @param delims delimiter policy
   * @return global clusters in label (creation) order
   * @throws IllegalStateException if the summaries were produced under differing fingerprints
   */
  public static List<GlobalCluster> merge(
      List<LocalClusterSummary> summaries, double threshold, MatchMode matchMode, String delims) {
    if (summaries == null || summaries.isEmpty()) {
      return List.of();
    }
    assertSingleFingerprint(summaries, threshold, matchMode, delims);

    List<LocalClusterSummary> ordered = new ArrayList<>(summaries);
    ordered.sort(
        Comparator.comparingInt(LocalClusterSummary::shardOrdinal)
            .thenComparingInt(LocalClusterSummary::localCreationOrdinal));

    TextSimilarityClustering similarity =
        new TextSimilarityClustering(threshold, matchMode, delims);
    List<MutableCluster> globals = new ArrayList<>();

    for (LocalClusterSummary summary : ordered) {
      String repText = summary.representativeText() != null ? summary.representativeText() : "";
      double bestSimilarity = -1.0;
      MutableCluster best = null;
      for (MutableCluster candidate : globals) {
        double sim = similarity.computeSimilarity(repText, candidate.representativeText);
        if (sim > bestSimilarity) {
          bestSimilarity = sim;
          best = candidate;
        }
      }
      if (best != null && bestSimilarity >= threshold - THRESHOLD_EPSILON) {
        best.count += summary.localCount();
      } else {
        globals.add(
            new MutableCluster(
                globals.size() + 1, repText, summary.representativeRow(), summary.localCount()));
      }
    }

    List<GlobalCluster> result = new ArrayList<>(globals.size());
    for (MutableCluster g : globals) {
      result.add(new GlobalCluster(g.label, g.representativeRow, g.count));
    }
    return result;
  }

  private static void assertSingleFingerprint(
      List<LocalClusterSummary> summaries, double threshold, MatchMode matchMode, String delims) {
    String expected = ClusterConfigFingerprint.of(matchMode, threshold, delims);
    for (LocalClusterSummary summary : summaries) {
      String fp = summary.configFingerprint();
      if (fp != null && !expected.equals(fp)) {
        throw new IllegalStateException(
            "Refusing to merge cluster summaries with mismatched configuration fingerprints: "
                + "expected ["
                + expected
                + "] but a summary carried ["
                + fp
                + "]");
      }
    }
  }

  private static final class MutableCluster {
    final int label;
    final String representativeText;
    final Map<String, Object> representativeRow;
    long count;

    MutableCluster(int label, String representativeText, Map<String, Object> row, long count) {
      this.label = label;
      this.representativeText = representativeText;
      this.representativeRow = row;
      this.count = count;
    }
  }
}
