/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Reduce phase of the distributed map/reduce cluster aggregation. It merges the per-shard
 * representatives (see {@link ClusterPartialState}) into global clusters using the same greedy,
 * threshold-based similarity rule the single-node command uses, applied a second time over
 * representatives.
 *
 * <p>This is intentionally approximate. A representative stands in for all of its shard-local
 * members, representatives are compared greedily in arrival order, and each incoming representative
 * is only compared against the current global representatives. The result therefore depends on how
 * rows were sharded and is not equivalent to a single global pass over every row. This is the
 * fundamental tradeoff of distributing greedy clustering; see the design note for a worked
 * counterexample.
 */
public final class ClusterRepresentativeMerge {

  private ClusterRepresentativeMerge() {}

  /**
   * Merge shard-local representatives into global clusters.
   *
   * @param shardStates per-shard partial states, in the order the coordinator received them
   * @param threshold cosine similarity threshold, identical to the map-phase threshold
   * @param matchMode vectorization mode, identical to the map phase
   * @param delims token delimiters, identical to the map phase
   * @return global representatives with summed member counts
   */
  public static List<ClusterPartialState.Representative> merge(
      List<ClusterPartialState> shardStates, double threshold, MatchMode matchMode, String delims) {
    TextSimilarityClustering similarity =
        new TextSimilarityClustering(threshold, matchMode, delims);
    List<String> texts = new ArrayList<>();
    List<Long> counts = new ArrayList<>();

    for (ClusterPartialState state : shardStates) {
      for (ClusterPartialState.Representative rep : state.representatives()) {
        int best = -1;
        double bestSim = 0.0;
        for (int i = 0; i < texts.size(); i++) {
          double sim = similarity.computeSimilarity(rep.text(), texts.get(i));
          if (sim > bestSim) {
            bestSim = sim;
            best = i;
          }
        }
        if (best >= 0 && bestSim >= threshold - 1e-9) {
          counts.set(best, counts.get(best) + rep.count());
        } else {
          texts.add(rep.text());
          counts.add(rep.count());
        }
      }
    }

    List<ClusterPartialState.Representative> global = new ArrayList<>(texts.size());
    for (int i = 0; i < texts.size(); i++) {
      global.add(new ClusterPartialState.Representative(texts.get(i), counts.get(i)));
    }
    return global;
  }
}
